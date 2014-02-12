(ns esper-spike.esper
  (:import [com.espertech.esper.client
            Configuration
            UpdateListener
            EPStatement
            EPServiceProviderManager]))

(defonce esper (atom nil))

(defn create-esper []
  (when @esper
    (.destroy @esper))
  (reset! esper (EPServiceProviderManager/getDefaultProvider)))

(defn create-listener
  "Creates an UpdateListener proxy that can be attached to
  handle updates to Esper statements. fun will be called for
  each newEvent received."
  [fun & {:keys [iterate extract-map keywordize]
          :or {iterate true
               extract-map true
               keywordize true}}]
  (proxy [UpdateListener] []
    (update [newEvents oldEvents]
      (let [callback (comp
                      fun
                      (if keywordize clojure.walk/keywordize-keys identity)
                      (if extract-map #(into {} (.getProperties %)) identity))
            apply-fn (if iterate
                         #(doall (map %1 %2))
                         apply)]
        (try
          (apply-fn callback newEvents)
          (catch Exception e
            (println e)
            )
          )))))

(defn attach-listener
  "Attaches the listener to the statement."
  [statement listener]
  (.addListener statement listener))

(defn create-statement
  "Creates an Esper statement"
  ([statement]
     (.createEPL (.getEPAdministrator @esper) statement))
  ([service statement]
     (.createEPL (.getEPAdministrator service) statement))
  ([service statement name]
     (.createEPL (.getEPAdministrator service) statement name)))
