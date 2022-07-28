package toxiproxy

import (
	"fmt"
	"io"
	"net"

	"github.com/sirupsen/logrus"

	"github.com/Shopify/toxiproxy/v2/stream"
	"github.com/Shopify/toxiproxy/v2/toxics"
)

// ToxicLinks are single direction pipelines that connects an input and output via
// a chain of toxics. The chain always starts with a NoopToxic, and toxics are added
// and removed as they are enabled/disabled. New toxics are always added to the end
// of the chain.
//
//         NoopToxic  LatencyToxic
//             v           v
// Input > ToxicStub > ToxicStub > Output.
//
type ToxicLink struct {
	stubs     []*toxics.ToxicStub
	proxy     *Proxy
	toxics    *ToxicCollection
	input     *stream.ChanWriter
	output    *stream.ChanReader
	direction stream.Direction
}

func NewToxicLink(
	proxy *Proxy,
	collection *ToxicCollection,
	direction stream.Direction,
) *ToxicLink {
	link := &ToxicLink{
		stubs: make(
			[]*toxics.ToxicStub,
			len(collection.chain[direction]),
			cap(collection.chain[direction]),
		),
		proxy:     proxy,
		toxics:    collection,
		direction: direction,
	}
	// Initialize the link with ToxicStubs
	last := make(chan *stream.StreamChunk) // The first toxic is always a noop
	link.input = stream.NewChanWriter(last)
	for i := 0; i < len(link.stubs); i++ {
		var next chan *stream.StreamChunk
		if i+1 < len(link.stubs) {
			next = make(chan *stream.StreamChunk, link.toxics.chain[direction][i+1].BufferSize)
		} else {
			next = make(chan *stream.StreamChunk)
		}

		link.stubs[i] = toxics.NewToxicStub(last, next)
		last = next
	}
	link.output = stream.NewChanReader(last)
	return link
}

// Start the link with the specified toxics.
func (link *ToxicLink) Start(
	server *ApiServer,
	name string,
	source io.Reader,
	dest io.WriteCloser,
) {
	labels := []string{
		link.Direction(),
		link.proxy.Name,
		link.proxy.Listen,
		link.proxy.Upstream}

	go link.read(labels, server, source)

	for i, toxic := range link.toxics.chain[link.direction] {
		if stateful, ok := toxic.Toxic.(toxics.StatefulToxic); ok {
			link.stubs[i].State = stateful.NewState()
		}

		if _, ok := toxic.Toxic.(*toxics.ResetToxic); ok {
			if err := source.(*net.TCPConn).SetLinger(0); err != nil {
				logrus.WithFields(logrus.Fields{
					"name":  link.proxy.Name,
					"toxic": toxic.Type,
					"err":   err,
				}).Error("source: Unable to setLinger(ms)")
			}

			if err := dest.(*net.TCPConn).SetLinger(0); err != nil {
				logrus.WithFields(logrus.Fields{
					"name":  link.proxy.Name,
					"toxic": toxic.Type,
					"err":   err,
				}).Error("dest: Unable to setLinger(ms)")
			}
		}

		go link.stubs[i].Run(toxic)
	}

	go link.write(labels, name, server, dest)
}

// read copies bytes from a source to the link's input channel.
func (link *ToxicLink) read(
	metricLabels []string,
	server *ApiServer,
	source io.Reader,
) {
	bytes, err := io.Copy(link.input, source)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"name":  link.proxy.Name,
			"bytes": bytes,
			"err":   err,
		}).Warn("Source terminated")
	}
	if server.Metrics.proxyMetricsEnabled() {
		server.Metrics.ProxyMetrics.ReceivedBytesTotal.
			WithLabelValues(metricLabels...).Add(float64(bytes))
	}
	link.input.Close()
}

// write copies bytes from the link's output channel to a destination.
func (link *ToxicLink) write(
	metricLabels []string,
	name string,
	server *ApiServer,
	dest io.WriteCloser,
) {
	bytes, err := io.Copy(dest, link.output)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"name":  link.proxy.Name,
			"bytes": bytes,
			"err":   err,
		}).Warn("Destination terminated")
	}
	if server.Metrics.proxyMetricsEnabled() {
		server.Metrics.ProxyMetrics.SentBytesTotal.
			WithLabelValues(metricLabels...).Add(float64(bytes))
	}
	dest.Close()
	link.toxics.RemoveLink(name)
	link.proxy.RemoveConnection(name)
}

// Add a toxic to the end of the chain.
func (link *ToxicLink) AddToxic(toxic *toxics.ToxicWrapper) {
	i := len(link.stubs)

	newin := make(chan *stream.StreamChunk, toxic.BufferSize)
	link.stubs = append(link.stubs, toxics.NewToxicStub(newin, link.stubs[i-1].Output))

	// Interrupt the last toxic so that we don't have a race when moving channels
	if link.stubs[i-1].InterruptToxic() {
		link.stubs[i-1].Output = newin

		if stateful, ok := toxic.Toxic.(toxics.StatefulToxic); ok {
			link.stubs[i].State = stateful.NewState()
		}

		go link.stubs[i].Run(toxic)
		go link.stubs[i-1].Run(link.toxics.chain[link.direction][i-1])
	} else {
		// This link is already closed, make sure the new toxic matches
		link.stubs[i].Output = newin // The real output is already closed, close this instead
		link.stubs[i].Close()
	}
}

// Update an existing toxic in the chain.
func (link *ToxicLink) UpdateToxic(toxic *toxics.ToxicWrapper) {
	if link.stubs[toxic.Index].InterruptToxic() {
		go link.stubs[toxic.Index].Run(toxic)
	}
}

// Remove an existing toxic from the chain.
func (link *ToxicLink) RemoveToxic(toxic *toxics.ToxicWrapper) {
	i := toxic.Index
	fmt.Println("LINK REMOVE_TOXIC 1 ", i, link)

	if link.stubs[i].InterruptToxic() {
		fmt.Println("LINK REMOVE_TOXIC 2 ", i, link)
		cleanup, ok := toxic.Toxic.(toxics.CleanupToxic)
		if ok {
			fmt.Println("LINK REMOVE_TOXIC 3 ", i, link)
			cleanup.Cleanup(link.stubs[i])
			// Cleanup could have closed the stub.
			fmt.Println("LINK REMOVE_TOXIC 4 ", i, link)
			if link.stubs[i].Closed() {
				fmt.Println("LINK REMOVE_TOXIC RETURN A", i, link)
				return
			}
		}

		fmt.Println("LINK REMOVE_TOXIC 6 ", i, link)
		stop := make(chan bool)
		// Interrupt the previous toxic to update its output
		fmt.Println("LINK REMOVE_TOXIC 7 ", i, link)
		go func() {
			fmt.Println("LINK REMOVE_TOXIC 8 ", i, link)
			stop <- link.stubs[i-1].InterruptToxic()
			fmt.Println("LINK REMOVE_TOXIC 9 ", i, link)

		}()

		// Unblock the previous toxic if it is trying to flush
		// If the previous toxic is closed, continue flusing until we reach the end.
		interrupted := false
		stopped := false
		for !interrupted {
			fmt.Println("LINK REMOVE_TOXIC !INTERRUPTED ", i, link)
			select {
			case interrupted = <-stop:
				stopped = true
			case tmp := <-link.stubs[i].Input:
				if tmp == nil {
					link.stubs[i].Close()
					if !stopped {
						<-stop
					}
					fmt.Println("LINK REMOVE_TOXIC RETURN B", i, link)
					return
				}
				link.stubs[i].Output <- tmp
			}
		}
		fmt.Println("LINK REMOVE_TOXIC 10 ", i, link)

		// Empty the toxic's buffer if necessary
		for len(link.stubs[i].Input) > 0 {
			fmt.Println("LINK REMOVE_TOXIC LEN>0 ", i, len(link.stubs[i].Input))
			tmp := <-link.stubs[i].Input
			if tmp == nil {
				link.stubs[i].Close()
				fmt.Println("LINK REMOVE_TOXIC RETURN C", i, link)
				return
			}
			link.stubs[i].Output <- tmp
		}
		fmt.Println("LINK REMOVE_TOXIC 11 ", i, link)

		link.stubs[i-1].Output = link.stubs[i].Output
		link.stubs = append(link.stubs[:i], link.stubs[i+1:]...)
		fmt.Println("LINK REMOVE_TOXIC 12 ", i, link)

		go link.stubs[i-1].Run(link.toxics.chain[link.direction][i-1])
		fmt.Println("LINK REMOVE_TOXIC 13 ", i, link)

	}
	fmt.Println("LINK REMOVE_TOXIC RETURN Z", i, link)
}

// Direction returns the direction of the link (upstream or downstream).
func (link *ToxicLink) Direction() string {
	if link.direction == stream.Upstream {
		return "upstream"
	}
	return "downstream"
}
