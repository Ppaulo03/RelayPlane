package systemtest

// testutilCounter sums every sample of a metric family (counters/gauges).
func testutilCounter(e *Env, name string) float64 {
	fams, err := e.Metrics.Registry.Gather()
	if err != nil {
		e.T.Fatal(err)
	}
	var total float64
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			switch {
			case m.Counter != nil:
				total += m.Counter.GetValue()
			case m.Gauge != nil:
				total += m.Gauge.GetValue()
			case m.Histogram != nil:
				total += float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return total
}
