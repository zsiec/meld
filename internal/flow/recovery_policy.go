package flow

// epochPolicy converts receiver observations into a continuous allocation for
// bounded-epoch repair. It contains no sender, wire, or encoder dependencies,
// which keeps the decision rule independently testable.
type epochPolicy struct {
	demandQ8      int
	correlationQ8 int
	memoryQ8      int
	initialized   bool
}

type epochObservation struct {
	clean    bool
	offered  bool
	lossy    bool
	burstQ8  int
	outage   bool
	reactive bool
}

const (
	epochOutageMinSymbols = epochBlockSymbols
	epochBurstThresholdQ8 = 2 * burstQ8One
	epochDemandOne        = 256
	epochExploreDemandQ8  = epochDemandOne / epochBlockSymbols
	epochMemoryDemandQ8   = 3 * epochDemandOne / 4
	epochCorrelationStep  = (epochDemandOne + 2) / 3
	epochLongSlackScale   = 75_000
	epochMinShare         = 1.0 / epochBlockSymbols
	epochReactiveScale    = 0.125

	// Cold exploration admits a row costing at most two recent sources. Once
	// correlated-loss memory is established, the crossover expands to three.
	epochProbeMaxSourceCostMultiple = 2
	epochMaxSourceCostMultiple      = 3
)

func (p *epochPolicy) reset() {
	*p = epochPolicy{}
}

// observe updates policy state from one feedback interval. Idle intervals carry
// no channel evidence and therefore preserve established state.
func (p *epochPolicy) observe(o epochObservation) {
	coldStart := !p.initialized
	p.initialized = true
	target := 0
	correlationDecay := (epochCorrelationStep + 7) / 8
	if !o.outage && o.offered {
		memoryDecay := 1
		if o.reactive {
			memoryDecay = correlationDecay
		}
		p.memoryQ8 = max(0, p.memoryQ8-memoryDecay)
	}
	switch {
	case o.outage:
		p.memoryQ8 = epochDemandOne
		target = epochDemandOne
	case o.lossy && o.burstQ8 >= epochBurstThresholdQ8:
		p.correlationQ8 = min(epochDemandOne, p.correlationQ8+epochCorrelationStep)
		if p.correlationQ8 == epochDemandOne {
			p.memoryQ8 = epochDemandOne
		}
		target = max(epochCorrelationTarget(p.correlationQ8), epochCorrelationTarget(p.memoryQ8))
	case o.lossy:
		p.correlationQ8 = max(0, p.correlationQ8-epochCorrelationStep)
		target = max(epochCorrelationTarget(p.correlationQ8), epochCorrelationTarget(p.memoryQ8))
	case o.clean && o.offered:
		p.correlationQ8 = max(0, p.correlationQ8-epochCorrelationStep)
		if p.correlationQ8 > epochCorrelationStep || p.memoryQ8 > epochCorrelationStep {
			target = max(epochCorrelationTarget(p.correlationQ8), epochCorrelationTarget(p.memoryQ8))
		}
	default:
		if !coldStart {
			return
		}
	}
	if coldStart && target < epochMemoryDemandQ8 {
		target = epochMemoryDemandQ8
	}
	if target > p.demandQ8 {
		decisive := coldStart || o.outage || p.correlationQ8 == epochDemandOne
		if decisive {
			p.demandQ8 = target
		} else {
			p.demandQ8 += (3*(target-p.demandQ8) + 3) / 4
		}
	} else if target < p.demandQ8 {
		releaseDivisor := 16
		if o.reactive || target <= epochExploreDemandQ8 {
			releaseDivisor = 4
		}
		delta := (p.demandQ8 - target + releaseDivisor - 1) / releaseDivisor
		if delta < 1 {
			delta = 1
		}
		p.demandQ8 -= delta
	}
	p.demandQ8 = min(max(p.demandQ8, 0), epochDemandOne)
}

func epochCorrelationTarget(confidenceQ8 int) int {
	confirmed := max(confidenceQ8-epochCorrelationStep, 0)
	return epochExploreDemandQ8 +
		(epochMemoryDemandQ8-epochExploreDemandQ8)*confirmed/
			(epochDemandOne-epochCorrelationStep)
}

// share maps demand onto the fraction of proactive credit assigned to epoch
// repair. Reactive feasibility and excess slack reduce that fraction smoothly.
func (p epochPolicy) share(reactive bool, slackMicros int64) float64 {
	if p.demandQ8 <= 0 {
		return 0
	}
	demand := float64(max(p.demandQ8-epochExploreDemandQ8, 0)) /
		float64(epochMemoryDemandQ8-epochExploreDemandQ8)
	if demand > 1 {
		demand = 1
	}
	share := epochMinShare + (1-epochMinShare)*demand
	if reactive {
		share *= epochReactiveScale
	}
	if slackMicros > epochLongSlackScale {
		share *= float64(epochLongSlackScale) / float64(slackMicros)
	}
	if share < epochMinShare {
		return epochMinShare
	}
	if share > 1 {
		return 1
	}
	return share
}

func (p epochPolicy) sourceCostMultiple() int64 {
	if p.memoryQ8 > epochCorrelationStep {
		return epochMaxSourceCostMultiple
	}
	return epochProbeMaxSourceCostMultiple
}
