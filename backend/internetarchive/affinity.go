// read affinity: route reads directly to a chosen copy of an item
// (primary/secondary datanode or alternate replica) resolved from the
// metadata record, with fallback to the download redirector.
package internetarchive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

// datacenter codes by hostname prefix, longest prefixes first
// primary: petabox Metadata/Datanode/Util.inc
// alternates: petabox Metadata/Locate/AlternateItemLocator.inc
// dn76 = eu observed in live MDAPI data, not in the petabox maps
var dcPrefixes = []struct{ prefix, dc string }{
	{"dn70", "bcdc"}, {"dn71", "bcdc"}, {"dn72", "bcdc"}, {"dn73", "bcdc"},
	{"dn76", "eu"},
	{"dn78", "uvic"},
	{"dn79", "330wps"},
	{"ia6", "dc6"}, {"dn6", "dc6"}, {"iw6", "dc6"},
	{"ia8", "dc8"}, {"ia9", "dc8"}, {"dn8", "dc8"}, {"dn9", "dc8"}, {"iw8", "dc8"}, {"iw9", "dc8"},
	{"ia2", "dc2"},
}

func serverDatacenter(server string) string {
	host, _, _ := strings.Cut(server, ".")
	for _, p := range dcPrefixes {
		if strings.HasPrefix(host, p.prefix) {
			return p.dc
		}
	}
	return "unknown"
}

func knownDatacenter(dc string) bool {
	for _, p := range dcPrefixes {
		if p.dc == dc {
			return true
		}
	}
	return false
}

// normalizeServer completes a short server name to an FQDN; names that
// already carry a domain are kept as MDAPI returned them
func normalizeServer(server string, alternate bool) string {
	if server == "" || strings.Contains(server, ".") {
		return server
	}
	switch serverDatacenter(server) {
	case "eu":
		return server + ".eu.archive.org"
	case "bcdc", "uvic", "330wps":
		return server + ".ca.archive.org"
	default:
		if alternate {
			// petabox appends .ca for short alternate names
			return server + ".ca.archive.org"
		}
		return server + ".us.archive.org"
	}
}

// affinitySelector is the parsed read_affinity option; exactly one of
// class/dc is set
type affinitySelector struct {
	class string // "primary", "secondary" or "alternate"
	dc    string // datacenter code for "dc:<code>"
}

func parseReadAffinity(s string) (*affinitySelector, error) {
	switch s {
	case "":
		return nil, nil
	case "primary", "secondary", "alternate":
		return &affinitySelector{class: s}, nil
	}
	if dc, ok := strings.CutPrefix(s, "dc:"); ok && dc != "" {
		return &affinitySelector{dc: dc}, nil
	}
	return nil, fmt.Errorf("invalid read_affinity %q: expected primary, secondary, alternate or dc:<code>", s)
}

// readCandidate is one directly readable copy of an item
type readCandidate struct {
	Class    string `json:"class"`    // primary, secondary, alternate
	Server   string `json:"server"`   // FQDN
	Dir      string `json:"dir"`      // item path on the node
	DC       string `json:"dc"`       // datacenter code, "unknown" if unrecognized
	Workable bool   `json:"workable"` // health per the metadata record
}

func (c readCandidate) url() string {
	return "https://" + c.Server + c.Dir
}

// resolveCandidates builds the ladder of direct-read copies from a
// metadata record: primary, secondary, then alternates
func resolveCandidates(m *MetadataResponse) []readCandidate {
	var out []readCandidate
	// workable_servers is the healthy subset of d1/d2; absent = assume healthy
	workable := func(server string) bool {
		return len(m.WorkableServers) == 0 || slices.Contains(m.WorkableServers, server)
	}
	if m.D1 != "" && m.Dir != "" {
		s := normalizeServer(m.D1, false)
		out = append(out, readCandidate{"primary", s, m.Dir, serverDatacenter(s), workable(m.D1)})
	}
	if m.D2 != "" && m.Dir != "" {
		s := normalizeServer(m.D2, false)
		out = append(out, readCandidate{"secondary", s, m.Dir, serverDatacenter(s), workable(m.D2)})
	}
	alts := m.AlternateLocations.Workable
	altWorkable := true
	if len(alts) == 0 {
		// stale records may lack the health-filtered list
		alts = m.AlternateLocations.Servers
		altWorkable = false
	}
	for _, a := range alts {
		if a.Server == "" || a.Dir == "" {
			continue
		}
		s := normalizeServer(a.Server, true)
		out = append(out, readCandidate{"alternate", s, a.Dir, serverDatacenter(s), altWorkable})
	}
	return out
}

// filterByAffinity returns matching candidates, workable copies first,
// otherwise keeping ladder order
func filterByAffinity(cands []readCandidate, sel *affinitySelector) []readCandidate {
	if sel == nil {
		return nil
	}
	var out []readCandidate
	for _, c := range cands {
		if sel.class != "" && c.Class != sel.class {
			continue
		}
		if sel.dc != "" && c.DC != sel.dc {
			continue
		}
		out = append(out, c)
	}
	slices.SortStableFunc(out, func(a, b readCandidate) int {
		switch {
		case a.Workable == b.Workable:
			return 0
		case a.Workable:
			return -1
		default:
			return 1
		}
	})
	return out
}

// openDirect reads from a copy matching read_affinity, walking down the
// candidate ladder on failure; 401/403 aborts the ladder since the
// datanodes share auth handling and retrying every copy is pointless
func (o *Object) openDirect(ctx context.Context, options []fs.OpenOption) (io.ReadCloser, error) {
	bucket, bucketPath := o.split()
	m, err := o.fs.requestMetadata(ctx, bucket)
	if err != nil {
		return nil, err
	}
	cands := filterByAffinity(resolveCandidates(m), o.fs.affinity)
	if len(cands) == 0 {
		return nil, fmt.Errorf("no copy matches read_affinity %q", o.fs.opt.ReadAffinity)
	}
	var lastErr error
	for _, c := range cands {
		opts := rest.Opts{
			Method:  "GET",
			RootURL: c.url(),
			Path:    "/" + rest.URLPathEscapeAll(bucketPath),
			Options: options,
		}
		var resp *http.Response
		err := o.fs.pacer.Call(func() (bool, error) {
			var err error
			resp, err = o.fs.front.Call(ctx, &opts)
			// the ladder is the retry mechanism, don't hammer one node
			return false, err
		})
		if err == nil {
			fs.Debugf(o, "direct read from %s copy %s", c.Class, c.Server)
			return resp.Body, nil
		}
		lastErr = err
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return nil, fmt.Errorf("direct read auth rejected by %s: %w", c.Server, err)
		}
		fs.Debugf(o, "direct read from %s failed: %v", c.Server, err)
	}
	// every copy failed -- locations may be stale, refetch on next open
	o.fs.invalidateMetadata(bucket)
	return nil, lastErr
}

var commandHelp = []fs.CommandHelp{{
	Name:  "locate",
	Short: "Show the serving copies of an item",
	Long: `This command resolves the serving copies of an item (primary/secondary
datanodes and alternate replicas) from its metadata record and shows
which of them the current read_affinity setting selects, in the order
reads would try them.

Usage:

    rclone backend locate remote:item
    rclone backend locate remote: item1 item2

It returns a JSON list with one object per item.
`,
}}

// Command executes backend-specific commands
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out any, err error) {
	switch name {
	case "locate":
		buckets := arg
		if len(buckets) == 0 {
			bucket, _ := f.split("")
			if bucket == "" {
				return nil, errors.New("locate: need an item in the remote or as an argument")
			}
			buckets = []string{bucket}
		}
		type locateResult struct {
			Item     string          `json:"item"`
			Affinity string          `json:"affinity,omitempty"`
			Copies   []readCandidate `json:"copies"`
			Selected []readCandidate `json:"selected,omitempty"`
		}
		results := []locateResult{}
		for _, arg := range buckets {
			bucket := f.opt.Enc.FromStandardName(arg)
			m, err := f.requestMetadata(ctx, bucket)
			if err != nil {
				return nil, fmt.Errorf("locate %q: %w", arg, err)
			}
			cands := resolveCandidates(m)
			results = append(results, locateResult{
				Item:     arg,
				Affinity: f.opt.ReadAffinity,
				Copies:   cands,
				Selected: filterByAffinity(cands, f.affinity),
			})
		}
		return results, nil
	default:
		return nil, fs.ErrorCommandNotFound
	}
}
