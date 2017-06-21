package kapacitor

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type SampleNode struct {
	node
	s *pipeline.SampleNode

	countsMu sync.RWMutex

	counts   map[models.GroupID]int64
	duration time.Duration
}

// Create a new  SampleNode which filters data from a source.
func newSampleNode(et *ExecutingTask, n *pipeline.SampleNode, l *log.Logger) (*SampleNode, error) {
	sn := &SampleNode{
		node:     node{Node: n, et: et, logger: l},
		s:        n,
		counts:   make(map[models.GroupID]int64),
		duration: n.Duration,
	}
	sn.node.runF = sn.runSample
	if n.Duration == 0 && n.N == 0 {
		return nil, errors.New("invalid sample rate: must be positive integer or duration")
	}
	return sn, nil
}

func (s *SampleNode) runSample([]byte) error {

	ins := NewLegacyEdges(s.ins)
	outs := NewLegacyEdges(s.outs)

	valueF := func() int64 {
		s.countsMu.RLock()
		l := len(s.counts)
		s.countsMu.RUnlock()
		return int64(l)
	}
	s.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	switch s.Wants() {
	case pipeline.StreamEdge:
		for p, ok := ins[0].NextPoint(); ok; p, ok = ins[0].NextPoint() {
			s.timer.Start()
			if s.shouldKeep(p.Group, p.Time) {
				s.timer.Pause()
				for _, child := range outs {
					err := child.CollectPoint(p)
					if err != nil {
						return err
					}
				}
				s.timer.Resume()
			}
			s.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := ins[0].NextBatch(); ok; b, ok = ins[0].NextBatch() {
			s.timer.Start()
			if s.shouldKeep(b.Group, b.TMax) {
				s.timer.Pause()
				for _, child := range outs {
					err := child.CollectBatch(b)
					if err != nil {
						return err
					}
				}
				s.timer.Resume()
			}
			s.timer.Stop()
		}
	}
	return nil
}

func (s *SampleNode) shouldKeep(group models.GroupID, t time.Time) bool {
	if s.duration != 0 {
		keepTime := t.Truncate(s.duration)
		return t.Equal(keepTime)
	} else {
		s.countsMu.Lock()
		count := s.counts[group]
		keep := count%s.s.N == 0
		count++
		s.counts[group] = count
		s.countsMu.Unlock()
		return keep
	}
}
