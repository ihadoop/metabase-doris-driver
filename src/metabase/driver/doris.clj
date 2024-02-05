(ns metabase.driver.doris
  "doris driver. Builds off of the SQL-JDBC driver."
  (:require
   [clojure.java.io :as jio]
   [clojure.java.jdbc :as jdbc]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [honey.sql :as sql]
   [java-time.api :as t]
   [medley.core :as m]
   [metabase.config :as config]
   [metabase.db.spec :as mdb.spec]
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.query-processor.util :as sql.qp.u]
   [metabase.driver.sql.util :as sql.u]
   [metabase.driver.sql.util.unprepare :as unprepare]
   [metabase.lib.field :as lib.field]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.timezone :as qp.timezone]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.upload :as upload]
   [metabase.util :as u]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.i18n :refer [deferred-tru trs]]
   [metabase.util.log :as log])
  (:import
   (java.io File)
   (java.sql Connection ResultSet ResultSetMetaData Types)
   (java.time LocalDateTime OffsetDateTime OffsetTime ZonedDateTime)))

(set! *warn-on-reflection* true)



(driver/register! :doris, :parent :sql-jdbc)


(defmethod driver/display-name :doris [_] "Doris")

(doseq [[feature supported?] {:foreign-keys                           false
                              :persist-models                         true
                              :convert-timezone                       false
                              :datetime-diff                          false
                              :now                                    true
                              :regex                                  false
                              :percentile-aggregations                false
                              :full-join                              false
                              :uploads                                false
                              :schemas                                false
                              :case-sensitivity-string-filter-options false
                              :index-info                             false}]
  (defmethod driver/database-supports? [:doris feature] [_driver _feature _db] supported?))


(defmethod driver/database-supports? [:doris :nested-field-columns] [_driver _feat db]
  (driver.common/json-unfolding-default db))

(doseq [feature [:actions :actions/custom]]
  (defmethod driver/database-supports? [:doris feature]
    [driver _feat _db]
    ;; Only supported for doris right now.
    (= driver :doris)))


(defmethod driver/database-supports? [:doris :table-privileges]
  [driver _feat db]
  ( = driver :doris))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(def default-ssl-cert-details
  "Server SSL certificate chain, in PEM format."
  {:name         "ssl-cert"
   :display-name (deferred-tru "Server SSL certificate chain")
   :placeholder  ""
   :visible-if   {"ssl" true}})

(defmethod driver/connection-properties :doris
  [_]
  (->>
   [driver.common/default-host-details
    (assoc driver.common/default-port-details :placeholder 3306)
    driver.common/default-dbname-details
    driver.common/default-user-details
    driver.common/default-password-details
    driver.common/cloud-ip-address-info
    driver.common/default-ssl-details
    default-ssl-cert-details
    driver.common/ssh-tunnel-preferences
    driver.common/advanced-options-start
    driver.common/json-unfolding
    (assoc driver.common/additional-options
           :placeholder  "tinyInt1isBit=false")
    driver.common/default-advanced-options]
   (map u/one-or-many)
   (apply concat)))

(defmethod sql.qp/add-interval-honeysql-form :doris
  [driver hsql-form amount unit]
  ;; doris doesn't support `:millisecond` as an option, but does support fractional seconds
  (if (= unit :millisecond)
    (recur driver hsql-form (/ amount 1000.0) :second)
    [:date_add hsql-form [:raw (format "INTERVAL %s %s" amount (name unit))]]))

;; now() returns current timestamp in seconds resolution; now(6) returns it in nanosecond resolution
(defmethod sql.qp/current-datetime-honeysql-form :doris
  [_]
  (h2x/with-database-type-info [:now [:inline 6]] "timestamp"))

(defmethod driver/humanize-connection-error-message :doris
  [_ message]
  (condp re-matches message
    #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
    :cannot-connect-check-host-and-port

    #"^Unknown database .*$"
    :database-name-incorrect

    #"Access denied for user.*$"
    :username-or-password-incorrect

    #"Must specify port after ':' in connection string"
    :invalid-hostname

    ;; else
    message))

#_{:clj-kondo/ignore [:deprecated-var]}
(defmethod sql-jdbc.sync/db-default-timezone :doris
  [_ spec]
  (let [sql                                    (str "SELECT @@GLOBAL.time_zone AS global_tz,"
                                                    " @@system_time_zone AS system_tz,"
                                                    " time_format("
                                                    "   timediff("
                                                    "      now(), convert_tz(now(), @@GLOBAL.time_zone, '+00:00')"
                                                    "   ),"
                                                    "   '%H:%i'"
                                                    " ) AS 'offset';")
        [{:keys [global_tz system_tz offset]}] (jdbc/query spec sql)
        the-valid-id                           (fn [zone-id]
                                                 (when zone-id
                                                   (try
                                                     (.getId (t/zone-id zone-id))
                                                     (catch Throwable _))))]
    (or
     ;; if global timezone ID is 'SYSTEM', then try to use the system timezone ID
     (when (= global_tz "SYSTEM")
       (the-valid-id system_tz))
     ;; otherwise try to use the global ID
     (the-valid-id global_tz)
     ;; failing that, calculate the offset between now in the global timezone and now in UTC. Non-negative offsets
     ;; don't come back with `+` so add that if needed
     (if (str/starts-with? offset "-")
       offset
       (str \+ offset)))))

(defmethod driver/db-start-of-week :doris
  [_]
  :sunday)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/unix-timestamp->honeysql [:doris :seconds] [_ _ expr]
  [:from_unixtime expr])

(defmethod sql.qp/cast-temporal-string [:doris :Coercion/ISO8601->DateTime]
  [_driver _coercion-strategy expr]
  (h2x/->datetime expr))

(defmethod sql.qp/cast-temporal-string [:doris :Coercion/YYYYMMDDHHMMSSString->Temporal]
  [_driver _coercion-strategy expr]
  [:convert expr [:raw "DATETIME"]])

(defmethod sql.qp/cast-temporal-byte [:doris :Coercion/YYYYMMDDHHMMSSBytes->Temporal]
  [driver _coercion-strategy expr]
  (sql.qp/cast-temporal-string driver :Coercion/YYYYMMDDHHMMSSString->Temporal expr))

(defn- date-format [format-str expr]
  [:date_format expr (h2x/literal format-str)])

(defn- str-to-date
  [format-str expr]
  (let [contains-date-parts? (some #(str/includes? format-str %)
                                   ["%a" "%b" "%c" "%D" "%d" "%e" "%j" "%M" "%m" "%U"
                                    "%u" "%V" "%v" "%W" "%w" "%X" "%x" "%Y" "%y"])
        contains-time-parts? (some #(str/includes? format-str %)
                                   ["%f" "%H" "%h" "%I" "%i" "%k" "%l" "%p" "%r" "%S" "%s" "%T"])
        database-type        (cond
                               (and contains-date-parts? (not contains-time-parts?)) "date"
                               (and contains-time-parts? (not contains-date-parts?)) "time"
                               :else                                                 "datetime")]
    (-> [:str_to_date expr (h2x/literal format-str)]
        (h2x/with-database-type-info database-type))))

(defmethod sql.qp/->float :doris
  [_ value]
  ;; no-op as doris doesn't support cast to float
  value)

(defmethod sql.qp/->integer :doris
  [_ value]
  (h2x/maybe-cast :signed value))

(defmethod sql.qp/->honeysql [:doris :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

(defmethod sql.qp/->honeysql [:doris :length]
  [driver [_ arg]]
  [:char_length (sql.qp/->honeysql driver arg)])

(def ^:private database-type->doris-cast-type-name
  {"integer"          "signed"
   "text"             "char"
   "double precision" "double"
   "bigint"           "unsigned"})

(defmethod sql.qp/json-query :doris
  [_driver unwrapped-identifier stored-field]
  {:pre [(h2x/identifier? unwrapped-identifier)]}
  (letfn [(handle-name [x] (str "\"" (if (number? x) (str x) (name x)) "\""))]
    (let [field-type            (:database-type stored-field)
          field-type            (get database-type->doris-cast-type-name field-type field-type)
          nfc-path              (:nfc-path stored-field)
          parent-identifier     (sql.qp.u/nfc-field->parent-identifier unwrapped-identifier stored-field)
          jsonpath-query        (format "$.%s" (str/join "." (map handle-name (rest nfc-path))))
          json-extract+jsonpath [:json_extract parent-identifier jsonpath-query]]
      (case (u/lower-case-en field-type)
        "timestamp" [:convert
                     [:str_to_date json-extract+jsonpath "\"%Y-%m-%dT%T.%fZ\""]
                     [:raw "DATETIME"]]
        "boolean" json-extract+jsonpath
        ("float" "double") [:+ json-extract+jsonpath [:inline 0.0]]

        [:convert json-extract+jsonpath [:raw (u/upper-case-en field-type)]]))))

(defmethod sql.qp/->honeysql [:doris :field]
  [driver [_ id-or-name opts :as mbql-clause]]
  (let [stored-field  (when (integer? id-or-name)
                        (lib.metadata/field (qp.store/metadata-provider) id-or-name))
        parent-method (get-method sql.qp/->honeysql [:sql :field])
        honeysql-expr (parent-method driver mbql-clause)]
    (cond
      (not (lib.field/json-field? stored-field))
      honeysql-expr

      (::sql.qp/forced-alias opts)
      (keyword (::add/source-alias opts))

      :else
      (walk/postwalk #(if (h2x/identifier? %)
                        (sql.qp/json-query :doris % stored-field)
                        %)
                     honeysql-expr))))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str (h2x/->datetime expr))))

(defn- ->date [expr]
  (if (h2x/is-of-type? expr "date")
    expr
    (-> [:date expr]
        (h2x/with-database-type-info "date"))))

(defn make-date
  "Create and return a date based on  a year and a number of days value."
  [year-expr number-of-days]
  (-> [:makedate year-expr (sql.qp/inline-num number-of-days)]
      (h2x/with-database-type-info "date")))

(defmethod sql.qp/date [:doris :minute]
  [_driver _unit expr]
  (let [format-str (if (= (h2x/database-type expr) "time")
                     "%H:%i"
                     "%Y-%m-%d %H:%i")]
    (trunc-with-format format-str expr)))

(defmethod sql.qp/date [:doris :hour]
  [_driver _unit expr]
  (let [format-str (if (= (h2x/database-type expr) "time")
                     "%H"
                     "%Y-%m-%d %H")]
    (trunc-with-format format-str expr)))

(defmethod sql.qp/date [:doris :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:doris :minute-of-hour]  [_ _ expr] (h2x/minute expr))
(defmethod sql.qp/date [:doris :hour-of-day]     [_ _ expr] (h2x/hour expr))
(defmethod sql.qp/date [:doris :day]             [_ _ expr] (->date expr))
(defmethod sql.qp/date [:doris :day-of-month]    [_ _ expr] [:dayofmonth expr])
(defmethod sql.qp/date [:doris :day-of-year]     [_ _ expr] [:dayofyear expr])
(defmethod sql.qp/date [:doris :month-of-year]   [_ _ expr] (h2x/month expr))
(defmethod sql.qp/date [:doris :quarter-of-year] [_ _ expr] (h2x/quarter expr))
(defmethod sql.qp/date [:doris :year]            [_ _ expr] (make-date (h2x/year expr) 1))

(defmethod sql.qp/date [:doris :day-of-week]
  [driver _unit expr]
  (sql.qp/adjust-day-of-week driver [:dayofweek expr]))


(defmethod sql.qp/date [:doris :week] [_ _ expr]
  (let [extract-week-fn (fn [expr]
                          (str-to-date "%X%V %W"
                                       (h2x/concat [:yearweek expr]
                                                   (h2x/literal " Sunday"))))]
    (sql.qp/adjust-start-of-week :doris extract-week-fn expr)))

(defmethod sql.qp/date [:doris :week-of-year-iso] [_ _ expr] (h2x/week expr 3))

(defmethod sql.qp/date [:doris :month] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (h2x/concat (date-format "%Y-%m" expr)
                           (h2x/literal "-01"))))


(defmethod sql.qp/date [:doris :quarter] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (h2x/concat (h2x/year expr)
                          (h2x/literal "-")
                          (h2x/- (h2x/* (h2x/quarter expr)
                                      3)
                                2)
                          (h2x/literal "-01"))))

(defmethod sql.qp/->honeysql [:doris :convert-timezone]
  [driver [_ arg target-timezone source-timezone]]
  (let [expr       (sql.qp/->honeysql driver arg)
        timestamp? (h2x/is-of-type? expr "timestamp")]
    (sql.u/validate-convert-timezone-args timestamp? target-timezone source-timezone)
    (h2x/with-database-type-info
     [:convert_tz expr (or source-timezone (qp.timezone/results-timezone-id)) target-timezone]
     "datetime")))

(defn- timestampdiff-dates [unit x y]
  [:timestampdiff [:raw (name unit)] (h2x/->date x) (h2x/->date y)])

(defn- timestampdiff [unit x y]
  [:timestampdiff [:raw (name unit)] x y])

(defmethod sql.qp/datetime-diff [:doris :year]    [_driver _unit x y] (timestampdiff-dates :year x y))
(defmethod sql.qp/datetime-diff [:doris :quarter] [_driver _unit x y] (timestampdiff-dates :quarter x y))
(defmethod sql.qp/datetime-diff [:doris :month]   [_driver _unit x y] (timestampdiff-dates :month x y))
(defmethod sql.qp/datetime-diff [:doris :week]    [_driver _unit x y] (timestampdiff-dates :week x y))
(defmethod sql.qp/datetime-diff [:doris :day]     [_driver _unit x y] [:datediff y x])
(defmethod sql.qp/datetime-diff [:doris :hour]    [_driver _unit x y] (timestampdiff :hour x y))
(defmethod sql.qp/datetime-diff [:doris :minute]  [_driver _unit x y] (timestampdiff :minute x y))
(defmethod sql.qp/datetime-diff [:doris :second]  [_driver _unit x y] (timestampdiff :second x y))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.sync/database-type->base-type :doris
  [_ database-type]
  (condp re-matches (u/lower-case-en (name database-type))
    #"boolean"          :type/Boolean
    #"tinyint"          :type/Integer
    #"smallint"         :type/Integer
    #"int"              :type/Integer
    #"bigint"           :type/BigInteger
    #"float"            :type/Float
    #"double"           :type/Float
    #"double precision" :type/Double
    #"decimal.*"        :type/Decimal
    #"char.*"           :type/Text
    #"varchar.*"        :type/Text
    #"string.*"         :type/Text
    #"binary*"          :type/*
    #"date"             :type/Date
    #"time"             :type/Time
    #"timestamp"        :type/DateTime
    #"interval"         :type/*
    #"array.*"          :type/Array
    #"map"              :type/Dictionary
    #".*"               :type/*))


(defmethod sql-jdbc.sync/column->semantic-type :doris
  [_ database-type _]
  ;; More types to be added when we start caring about them
  (case database-type
    "JSON"  :type/SerializedJSON
    nil))

(def ^:private default-connection-args
  "Map of args for the doris/MariaDB JDBC connection string."
  { ;; 0000-00-00 dates are valid in doris; convert these to `null` when they come back because they're illegal in Java
   :zeroDateTimeBehavior "convertToNull"
   ;; Force UTF-8 encoding of results
   :useUnicode           true
   :characterEncoding    "UTF8"
   :characterSetResults  "UTF8"
   ;; GZIP compress packets sent between Metabase server and doris/MariaDB database
   :useCompression       true})

(defn- maybe-add-program-name-option [jdbc-spec additional-options-map]
  (let [set-prog-nm-fn (fn []
                         (let [prog-name (str/replace config/mb-version-and-process-identifier "," "_")]
                           (assoc jdbc-spec :connectionAttributes (str "program_name:" prog-name))))]
    (if-let [conn-attrs (get additional-options-map "connectionAttributes")]
      (if (str/includes? conn-attrs "program_name")
        jdbc-spec ; additional-options already includes the program_name; don't set it here
        (set-prog-nm-fn))
      (set-prog-nm-fn)))) ; additional-options did not contain connectionAttributes at all; set it

(defn make-subname
  "Make a subname for the given `host`, `port`, and `db` params.  Iff `db` is not blank, then a slash will
  precede it in the subname."
  {:arglists '([host port db]), :added "0.39.0"}
  [host port db]
  (str "//" (when-not (str/blank? host) (str host ":" port)) (if-not (str/blank? db) (str "/" db) "/")))

(defmethod mdb.spec/spec :doris
  [_ {:keys [host port db]
      :or   {host "localhost", port 3306, db ""}
      :as   opts}]
  (merge
   {:classname   "com.mysql.cj.jdbc.Driver"
    :subprotocol "mysql"
    :subname     (make-subname host (or port 3306) db)}
   (dissoc opts :host :port :db)))


(defmethod driver/describe-table :doris
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query {:connection conn} [(format
                                                    "describe %s" table-name)])]
       (set
        (for [[idx result] (m/indexed results)]
          {:name              (get result :field)
           :database-type     (get result :type)
           :base-type        (sql-jdbc.sync/database-type->base-type driver (keyword (get result :type)))
           :database-position idx}))))})

(defmethod sql-jdbc.conn/connection-details->spec :doris
  [_ {ssl? :ssl, :keys [additional-options ssl-cert], :as details}]
  (let [addl-opts-map (sql-jdbc.common/additional-options->map additional-options :url "=" false)
        ssl?          (or ssl? (= "true" (get addl-opts-map "useSSL")))
        ssl-cert?     (and ssl? (some? ssl-cert))]
    (when (and ssl? (not (contains? addl-opts-map "trustServerCertificate")))
      (log/info (trs "You may need to add 'trustServerCertificate=true' to the additional connection options to connect with SSL.")))
    (merge
     default-connection-args
     ;; newer versions of doris will complain if you don't specify this when not using SSL
     {:useSSL (boolean ssl?)}
     (let [details (-> (if ssl-cert? (set/rename-keys details {:ssl-cert :serverSslCert}) details)
                       (set/rename-keys {:dbname :db})
                       (dissoc :ssl))]
       (-> (mdb.spec/spec :doris details)
           (maybe-add-program-name-option addl-opts-map)
           (sql-jdbc.common/handle-additional-options details))))))


(defmethod driver/describe-database :doris
  [driver database]
  {:tables
   (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (set
       (for [_map (jdbc/query {:connection conn} ["show tables"])]
         {:name   (first (vals _map)) ; column name differs depending on server (SparkSQL, hive, Impala)
          :schema  (get-in database [:details :dbname])}))))})




(defmethod sql-jdbc.sync/excluded-schemas :doris
  [_]
  #{"INFORMATION_SCHEMA"})

(defmethod sql.qp/quote-style :doris [_] :mysql)
;;
(defmethod sql-jdbc.execute/set-timezone-sql :doris
  [_]
  "SET @@session.time_zone = %s;")

(defmethod sql-jdbc.execute/set-parameter [:doris OffsetTime]
  [driver ps i t]
  ;; convert to a LocalTime so doris doesn't get F U S S Y
  (sql-jdbc.execute/set-parameter driver ps i (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))))

(defmethod sql-jdbc.execute/set-parameter [:doris OffsetDateTime]
  [driver ^java.sql.PreparedStatement ps ^Integer i t]
  (let [zone   (t/zone-id (qp.timezone/results-timezone-id))
        offset (.. zone getRules (getOffset (t/instant t)))
        t      (t/local-date-time (t/with-offset-same-instant t offset))]
    (sql-jdbc.execute/set-parameter driver ps i t)))

;; doris TIMESTAMPS are actually TIMESTAMP WITH LOCAL TIME ZONE, i.e. they are stored normalized to UTC when stored.
;; However, doris returns them in the report time zone in an effort to make our lives horrible.
(defmethod sql-jdbc.execute/read-column-thunk [:doris Types/TIMESTAMP]
  [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  ;; Check and see if the column type is `TIMESTAMP` (as opposed to `DATETIME`, which is the equivalent of
  ;; LocalDateTime), and normalize it to a UTC timestamp if so.
  (if (= (.getColumnTypeName rsmeta i) "TIMESTAMP")
    (fn read-timestamp-thunk []
      (when-let [t (.getObject rs i LocalDateTime)]
        (t/with-offset-same-instant (t/offset-date-time t (t/zone-id (qp.timezone/results-timezone-id))) (t/zone-offset 0))))
    (fn read-datetime-thunk []
      (.getObject rs i LocalDateTime))))


(defmethod sql-jdbc.execute/read-column-thunk [:doris Types/TIME]
  [driver ^ResultSet rs rsmeta ^Integer i]
  (let [parent-thunk ((get-method sql-jdbc.execute/read-column-thunk [:sql-jdbc Types/TIME]) driver rs rsmeta i)]
    (fn read-time-thunk []
      (try
        (parent-thunk)
        (catch Throwable _
          (.getString rs i))))))


(defmethod sql-jdbc.execute/read-column-thunk [:doris Types/DATE]
  [driver ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (if (= "YEAR" (.getColumnTypeName rsmeta i))
    (fn read-time-thunk []
      (when-let [x (.getObject rs i)]
        (.getYear (.toLocalDate ^java.sql.Date x))))
    (let [parent-thunk ((get-method sql-jdbc.execute/read-column-thunk [:sql-jdbc Types/DATE]) driver rs rsmeta i)]
      parent-thunk)))

(defn- format-offset [t]
  (let [offset (t/format "ZZZZZ" (t/zone-offset t))]
    (if (= offset "Z")
      "UTC"
      offset)))

(defmethod unprepare/unprepare-value [:doris OffsetTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:doris OffsetDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:doris ZonedDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (str (t/zone-id t))))

(defmethod driver/upload-type->database-type :doris
  [_driver upload-type]
  (case upload-type
    ::upload/varchar-255              [[:varchar 255]]
    ::upload/text                     [:text]
    ::upload/int                      [:bigint]
    ::upload/auto-incrementing-int-pk [:bigint :not-null :auto-increment :primary-key]
    ::upload/float                    [:double]
    ::upload/boolean                  [:boolean]
    ::upload/date                     [:date]
    ::upload/datetime                 [:datetime]
    ::upload/offset-datetime          [:timestamp]))

(defmethod driver/table-name-length-limit :doris
  [_driver]
  64)

(defn- format-load
  [_clause [file-path table-name]]
  [(format "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" file-path (sql/format-entity table-name))])

(sql/register-clause! ::load format-load :insert-into)


(defmulti ^:private value->string
  "Convert a value into a string that's safe for insertion"
  {:arglists '([driver val])}
  (fn [_ val] (type val)))

(defmethod value->string :default
  [_driver val]
  (str val))

(defmethod value->string nil
  [_driver _val]
  nil)

(defmethod value->string Boolean
  [_driver val]
  (if val
    "1"
    "0"))

(defmethod value->string LocalDateTime
  [_driver val]
  (t/format :iso-local-date-time val))


(defn- sanitize-value
  [v]
  (if (nil? v)
    "\\N"
    (str/replace v #"\\|\n|\r|\t" {"\\" "\\\\"
                                   "\n" "\\n"
                                   "\r" "\\r"
                                   "\t" "\\t"})))

(defn- row->tsv
  [driver column-count row]
  (when (not= column-count (count row))
    (throw (Exception. (format "ERROR: missing data in row \"%s\"" (str/join "," row)))))
  (->> row
       (map (comp sanitize-value (partial value->string driver)))
       (str/join "\t")))

(defn- get-global-variable
  "The value of the given global variable in the DB. Does not do any type coercion, so, e.g., booleans come back as
  \"ON\" and \"OFF\"."
  [db-id var-name]
  (:value
   (first
    (jdbc/query (sql-jdbc.conn/db->pooled-connection-spec db-id)
                ["show global variables like ?" var-name]))))

(defmethod driver/insert-into! :doris
  [driver db-id ^String table-name column-names values]
  (if (not= (get-global-variable db-id "local_infile") "ON")
    ;; If it isn't turned on, fall back to the generic "INSERT INTO ..." way
    ((get-method driver/insert-into! :sql-jdbc) driver db-id table-name column-names values)
    (let [temp-file (File/createTempFile table-name ".tsv")
          file-path (.getAbsolutePath temp-file)]
      (try
        (let [tsvs (map (partial row->tsv driver (count column-names)) values)
              sql  (sql/format {::load   [file-path (keyword table-name)]
                                :columns (map keyword column-names)}
                               :quoted  true
                               :dialect (sql.qp/quote-style driver))]
          (with-open [^java.io.Writer writer (jio/writer file-path)]
            (doseq [value (interpose \newline tsvs)]
              (.write writer (str value))))
          (sql-jdbc.execute/do-with-connection-with-options
           driver
           db-id
           nil
           (fn [conn]
             (jdbc/execute! {:connection conn} sql))))
        (finally
          (.delete temp-file))))))

(defmethod driver/current-user-table-privileges :doris
  [_driver database]
    (let [conn-spec   (sql-jdbc.conn/db->pooled-connection-spec database)
          table-names (->> (jdbc/query conn-spec "SHOW TABLES" {:as-arrays? true})
                           (drop 1)
                           (map first))]
      (for [table-name table-names]
        {:role   nil
         :schema nil
         :table  table-name
         :select true
         :update true
         :insert true
         :delete true})))
