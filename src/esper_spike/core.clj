(ns esper-spike.core
  (:import [com.espertech.esper.client
            Configuration
            UpdateListener
            EPStatement
            EPServiceProviderManager]))

(defn init-result-val [] {:once 0 :count [] :avg []})

(defonce esper (atom nil))
(defonce result (atom (init-result-val)))

(defn create-esper []
  (when @esper
    (.destroy @esper))
  (reset! esper (EPServiceProviderManager/getDefaultProvider)))

(defn create-schema []
  (.createEPL (.getEPAdministrator @esper)
              "create schema YNSpikeEvent (spike_int integer)"))

(defn create-once-listener []
  (let [stmt
        (.createEPL (.getEPAdministrator @esper)
                    "select count(*) from YNSpikeEvent having count(*) = 1")
        once-listener (proxy [UpdateListener] []
                        (update [newEvents oldEvents]
                          (swap! result update-in [:once] inc)))]
    (.addListener stmt once-listener)))

(defn create-avg-count-listener []
  (let [stmt
        (.createEPL (.getEPAdministrator @esper)
                    "select count(*) as c, avg(spike_int) as a from YNSpikeEvent")
        avg-count-listener (proxy [UpdateListener] []
                             (update [newEvents oldEvents]
                               (let [a (get (.getProperties (first newEvents)) "a")
                                     c (get (.getProperties (first newEvents)) "c")]
                                 (swap! result update-in [:count] conj c)
                                 (swap! result update-in [:avg] conj a))))]
    (.addListener stmt avg-count-listener)))

(defn send-events []
  (.sendEvent (.getEPRuntime @esper) {"spike_int" 5} "YNSpikeEvent")
  (.sendEvent (.getEPRuntime @esper) {"spike_int" 6} "YNSpikeEvent")
  (.sendEvent (.getEPRuntime @esper) {"spike_int" 10} "YNSpikeEvent"))

(defn go []
  (reset! result (init-result-val))
  (create-esper)
  (create-schema)
  (create-once-listener)
  (create-avg-count-listener)
  (send-events))
