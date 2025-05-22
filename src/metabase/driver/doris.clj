;copied from
;
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
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.models.secret :as secret]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.sync.describe-table :as sql-jdbc.describe-table]
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
   [metabase.driver.sql-jdbc.execute.legacy-impl :as sql-jdbc.legacy]
   [metabase.util.i18n :refer [deferred-tru trs]]
   [metabase.util.log :as log])
  (:import
   (java.io File)
   (java.sql Connection ResultSet ResultSetMetaData Types)
   (java.time LocalDateTime OffsetDateTime OffsetTime ZonedDateTime)))


(set! *warn-on-reflection* true)

(driver/register! :doris, :parent #{:sql-jdbc
                                    ::sql-jdbc.legacy/use-legacy-classes-for-read-and-set})

;;(defmethod driver/display-name :doris [_] "Doris")

(doseq [[feature supported?] {:set-timezone                    true
                              :basic-aggregations              true
                              :standard-deviation-aggregations true
                              :expressions                     true
                              :native-parameters               true
                              :expression-aggregations         true
                              :binning                         false
                              :foreign-keys                    false
                              :now                             true}]
  (defmethod driver/database-supports? [:doris feature] [_driver _feature _db] supported?))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(def default-ssl-cert-details
  "Server SSL certificate chain, in PEM format."
  {:name         "ssl-cert"
   :display-name (deferred-tru "Server SSL certificate chain")
   :placeholder  ""
   :visible-if   {"ssl" true}})

;; (defmethod driver/connection-properties :doris
;;   [_]
;;   (->>
;;    [driver.common/default-host-details
;;     (assoc driver.common/default-port-details :placeholder 3306)
;;     driver.common/default-dbname-details
;;     driver.common/default-user-details
;;     driver.common/default-password-details
;;     driver.common/cloud-ip-address-info
;;     driver.common/default-ssl-details
;;     default-ssl-cert-details
;;     driver.common/ssh-tunnel-preferences
;;     driver.common/advanced-options-start
;;     driver.common/json-unfolding
;;     (assoc driver.common/additional-options
;;            :placeholder  "tinyInt1isBit=false")
;;     driver.common/default-advanced-options]
;;    (map u/one-or-many)
;;    (apply concat)))

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
  "UTC")

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

(def ^:private ^{:arglists '([column-type])} doris-type->base-type
  "Function that returns a `base-type` for the given `doris-type` (can be a keyword or string)."
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [[#"(?i)BOOLEAN"                    :type/Boolean]
    [#"(?i)TINYINT"                    :type/Integer]
    [#"(?i)SMALLINT"                   :type/Integer]
    [#"(?i)INT"                        :type/Integer]
    [#"(?i)BIGINT"                     :type/BigInteger]
    [#"(?i)LARGEINT"                   :type/BigInteger]
    [#"(?i)FLOAT"                      :type/Float]
    [#"(?i)DOUBLE"                     :type/Float]
    [#"(?i)DECIMAL.*"                  :type/Decimal]

    [#"(?i)DATE"                       :type/Date]
    [#"(?i)DATETIME.*"                 :type/DateTime]

    [#"(?i)CHAR.*"                     :type/Text]
    [#"(?i)VARCHAR.*"                  :type/Text]
    [#"(?i)STRING"                     :type/Text]

    [#"(?i)ARRAY"                      :type/Array]
    [#"(?i)MAP"                        :type/Map]
    [#"(?i)STRUCT"                     :type/Dictionary]
    [#"(?i)JSON"                       :type/JSON] ; TODO - this should probably be Dictionary or something
    [#"(?i)VARIANT"                    :type/JSON] ; TODO - this should probably be Dictionary or something
    [#".*"                             :type/*]]))

(defmethod sql-jdbc.sync/database-type->base-type :doris
  [_ database-type]
  (condp re-matches (u/lower-case-en (name database-type))
    #"boolean"          :type/Boolean
    #"tinyint"          :type/Integer
    #"smallint"         :type/Integer
    #"int"              :type/Integer
    #"integer"          :type/Integer
    #"bigint"           :type/BigInteger
    #"largeint"         :type/BigInteger
    #"float"            :type/Float
    #"double"           :type/Float
    #"double precision" :type/Double
    #"decimal.*"        :type/Decimal
    #"char.*"           :type/Text
    #"varchar.*"        :type/Text
    #"string"           :type/Text
    #"text"             :type/Text
    #"date"             :type/Date
    #"time"             :type/Time
    #"datetime"         :type/DateTime
    #"timestamp"        :type/DateTime

    #"array.*"          :type/Array
    #"map"              :type/Map
    #"json"             :type/JSON
    #"variant"          :type/JSON
    #".*"               :type/*))

(defmethod sql-jdbc.sync/column->semantic-type :doris
  [_ database-type _]
  ;; More types to be added when we start caring about them
  (case database-type
    "JSON"  :type/SerializedJSON
    nil))

(def ^:private default-connection-args
  "Map of args for the doris/MariaDB JDBC connection string."
  {;; 0000-00-00 dates are valid in doris; convert these to `null` when they come back because they're illegal in Java
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

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Connectivity                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; Kerberos related definitions
(def ^:private kerb-props->url-param-names
  {:kerberos-principal "KerberosPrincipal"
   :kerberos-remote-service-name "KerberosRemoteServiceName"
   :kerberos-use-canonical-hostname "KerberosUseCanonicalHostname"
   :kerberos-credential-cache-path "KerberosCredentialCachePath"
   :kerberos-keytab-path "KerberosKeytabPath"
   :kerberos-service-principal-pattern "KerberosServicePrincipalPattern"
   :kerberos-config-path "KerberosConfigPath"})

(defn- details->kerberos-url-params [details]
  (let [remove-blank-vals (fn [m] (into {} (remove (comp str/blank? val) m)))
        ks                (keys kerb-props->url-param-names)]
    (-> (select-keys details ks)
        remove-blank-vals
        (set/rename-keys kerb-props->url-param-names))))

(defn- append-additional-options [additional-options props]
  (let [opts-str (sql-jdbc.common/additional-opts->string :url props)]
    (if (str/blank? additional-options)
      opts-str
      (str additional-options "&" opts-str))))

(defn- prepare-addl-opts [{:keys [SSL kerberos] :as details}]
  (let [det (if kerberos
              (if-not SSL
                (throw (ex-info (trs "SSL must be enabled to use Kerberos authentication")
                                {:db-details details}))
                ;; convert Kerberos options map to URL string
                (update details
                        :additional-options
                        append-additional-options
                        (details->kerberos-url-params details)))
              details)]
    ;; in any case, remove the standalone Kerberos properties from details map
    (apply dissoc (cons det (keys kerb-props->url-param-names)))))

(defn- db-name
  "Creates a \"DB name\" for the given catalog `c` and (optional) schema `s`.  If both are specified, a slash is
  used to separate them.  See examples at:
  https://dorisdb.io/docs/current/installation/jdbc.html#connecting"
  [c s]
  (cond
    (str/blank? c)
    s

    (str/blank? s)
    c

    :else
    (str c "." s)))

(defn- jdbc-spec
  "Creates a spec for `clojure.java.jdbc` to use for connecting to doris via JDBC, from the given `opts`."
  [{:keys [host port catalog schema]
    :or   {host "localhost", port 5432, catalog "default", schema "default"}
    :as   details}]

  (-> details
      (merge {:classname   "com.mysql.cj.jdbc.Driver"
              :subprotocol "mysql"
              :subname     (mdb.spec/make-subname host port (db-name catalog schema))})
      prepare-addl-opts
      (dissoc :host :port :db :catalog :schema :tunnel-enabled :engine :kerberos)
      sql-jdbc.common/handle-additional-options))

(defn- str->bool [v]
  (if (string? v)
    (Boolean/parseBoolean v)
    v))

(defn- get-valid-secret-file [details-map property-name]
  (let [file (secret/value-as-file! :doris  details-map property-name)]
    (when-not file
      (throw (ex-info (format "Property %s should be defined" property-name)
                      {:connection-details details-map
                       :property-name property-name})))
    (.getCanonicalPath file)))

(defn- maybe-add-ssl-stores [details-map]
  (let [props
        (cond-> {}
          (str->bool (:ssl-use-keystore details-map))
          (assoc :SSLKeyStorePath (get-valid-secret-file details-map "ssl-keystore")
                 :SSLKeyStorePassword (secret/value-as-string :doris details-map "ssl-keystore-password"))
          (str->bool (:ssl-use-truststore details-map))
          (assoc :SSLTrustStorePath (get-valid-secret-file details-map "ssl-truststore")
                 :SSLTrustStorePassword (secret/value-as-string :doris details-map "ssl-truststore-password")))]
    (cond-> details-map
      (seq props)
      (update :additional-options append-additional-options props))))

(defmethod sql-jdbc.conn/connection-details->spec :doris
  [_ details-map]
  (let [props (-> details-map
                  (update :port (fn [port]
                                  (if (string? port)
                                    (Integer/parseInt port)
                                    port)))
                  (update :ssl str->bool)
                  (update :kerberos str->bool)
                  (assoc :SSL (:ssl details-map))
                  maybe-add-ssl-stores
                  ;; remove any Metabase specific properties that are not recognized by the dorisDB JDBC driver, which is
                  ;; very picky about properties (throwing an error if any are unrecognized)
                  ;; all valid properties can be found in the JDBC Driver source here:
                  ;; https://github.com/dorisdb/doris/blob/master/tri/src/main/java/com/facebook/doris/jdbc/ConnectionProperties.java
                  (select-keys (concat
                                [:host :port :catalog :schema :additional-options ; needed for `jdbc-spec`
                                 ;; JDBC driver specific properties
                                 :kerberos ; we need our boolean property indicating if Kerberos is enabled
                                           ; but the rest of them come from `kerb-props->url-param-names` (below)
                                 :user :password :socksProxy :httpProxy :applicationNamePrefix :disableCompression :SSL
                                 ;; Passing :SSLKeyStorePath :SSLKeyStorePassword :SSLTrustStorePath :SSLTrustStorePassword
                                 ;; in the properties map doesn't seem to work, they are included as additional options.
                                 :accessToken :extraCredentials :sessionProperties :protocols :queryInterceptors]
                                (keys kerb-props->url-param-names))))]
    (jdbc-spec props)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Connectivity  - End                                           |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.sync/excluded-schemas :doris
  [_]
  #{"INFORMATION_SCHEMA"})

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

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           Sync  Method Impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- describe-catalog-sql
  "The SHOW SCHEMAS statement that will list all schemas for the given `catalog`."
  {:added "0.39.0"}
  [driver catalog]
  (str "SHOW SCHEMAS FROM " (sql.u/quote-name driver :database catalog)))

(defn- describe-schema-sql
  "The SHOW TABLES statement that will list all tables for the given `catalog` and `schema`."
  {:added "0.39.0"}
  [driver catalog schema]
  (str "SHOW TABLES FROM " (sql.u/quote-name driver :schema catalog schema)))

(defn- describe-table-sql
  "The DESCRIBE  statement that will list information about the given `table`, in the given `catalog` and schema`."
  {:added "0.39.0"}
  [driver catalog schema table]
  (str "DESCRIBE " (sql.u/quote-name driver :table catalog schema table)))

(defn- have-select-privilege?
  "Checks whether the connected user has permission to select from the given `table-name`, in the given `schema`.
  Adapted from the legacy doris driver implementation."
  [driver conn schema table-name]
  true)

(defn- describe-schema
  "Gets a set of maps for all tables in the given `catalog` and `schema`. Adapted from the legacy doris driver
  implementation."
  [driver conn catalog schema]
  (let [sql (describe-schema-sql driver catalog schema)]
    (log/info (trs "Running statement in describe-schema: {0}" sql))
    (into #{} (comp (filter (fn [{table-name :table}]
                              (have-select-privilege? driver conn schema table-name)))
                    (map (fn [{table-name :table}]
                           {:name        table-name
                            :schema      schema})))
          (jdbc/reducible-query {:connection conn} sql))))

(def ^:private excluded-schemas
  "The set of schemas that should be excluded when querying all schemas."
  #{"information_schema"})

(defn- all-schemas
  "Gets a set of maps for all tables in all schemas in the given `catalog`. Adapted from the legacy doris driver
  implementation."
  [driver conn catalog]
  (let [sql (describe-catalog-sql driver catalog)]
    (log/trace (trs "Running statement in all-schemas: {0}" sql))
    (into []
          (map (fn [{:keys [schema]}]
                 (when-not (contains? excluded-schemas schema)
                   (describe-schema driver conn catalog schema))))
          (jdbc/reducible-query {:connection conn} sql))))

(defmethod driver/describe-database :doris
  [driver {{:keys [catalog schema] :as _details} :details :as database}]
  {:tables
   (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (set
       (for [_map (jdbc/query {:connection conn} [(describe-schema-sql driver catalog schema)])]
         {:name   (first (vals _map)) ; column name differs depending on server (SparkSQL, hive, Impala)
          :schema  (get-in database [:details :schema])}))))})

(defmethod driver/describe-table :doris
  [driver {{:keys [catalog schema] :as _details} :details :as database} {table-name :name}]
  {:name   table-name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query {:connection conn} [(describe-table-sql driver catalog schema table-name)])]
       (set
        (for [[idx result] (m/indexed results)]
          {:name              (get result :field)
           :database-type     (get result :type)
           :base-type        (sql-jdbc.sync/database-type->base-type driver (keyword (get result :type)))
           :database-position idx}))))})

(defmethod sql-jdbc.describe-table/get-table-pks :doris
  [_driver ^Connection conn db-name-or-nil table]
  nil)

;;; The doris JDBC driver DOES NOT support the `.getImportedKeys` method so just return `nil` here so the `:sql-jdbc`
;;; implementation doesn't try to use it.
(defmethod driver/describe-table-fks :doris
  [_driver _database _table]
  nil)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           Other Driver Method Impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(prefer-method driver/database-supports? [:doris :set-timezone] [:sql-jdbc :set-timezone])

(defmethod sql.qp/quote-style :doris [_driver]
  :mysql)
