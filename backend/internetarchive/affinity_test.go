package internetarchive

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerDatacenter(t *testing.T) {
	for _, tc := range []struct{ server, dc string }{
		{"ia802907.us.archive.org", "dc8"},
		{"ia902907.us.archive.org", "dc8"},
		{"ia601234.us.archive.org", "dc6"},
		{"ia601234", "dc6"},
		{"iw601234", "dc6"},
		{"dn601234", "dc6"},
		{"ia201234.us.archive.org", "dc2"},
		{"dn710607.ca.archive.org", "bcdc"},
		{"dn730001", "bcdc"},
		{"dn760109.eu.archive.org", "eu"},
		{"dn780001.ca.archive.org", "uvic"},
		{"dn790001", "330wps"},
		{"dn740001", "unknown"},
		{"something.else.org", "unknown"},
		{"", "unknown"},
	} {
		assert.Equal(t, tc.dc, serverDatacenter(tc.server), tc.server)
	}
}

func TestNormalizeServer(t *testing.T) {
	// FQDNs pass through untouched
	assert.Equal(t, "ia802907.us.archive.org", normalizeServer("ia802907.us.archive.org", false))
	assert.Equal(t, "dn760109.eu.archive.org", normalizeServer("dn760109.eu.archive.org", true))
	// short names get a domain inferred from the datacenter
	assert.Equal(t, "ia802907.us.archive.org", normalizeServer("ia802907", false))
	assert.Equal(t, "dn710607.ca.archive.org", normalizeServer("dn710607", true))
	assert.Equal(t, "dn760109.eu.archive.org", normalizeServer("dn760109", true))
	// unknown prefix: .ca for alternates (petabox behavior), .us otherwise
	assert.Equal(t, "dn740001.ca.archive.org", normalizeServer("dn740001", true))
	assert.Equal(t, "xx123.us.archive.org", normalizeServer("xx123", false))
	assert.Equal(t, "", normalizeServer("", false))
}

func TestParseReadAffinity(t *testing.T) {
	sel, err := parseReadAffinity("")
	require.NoError(t, err)
	assert.Nil(t, sel)

	for _, class := range []string{"primary", "secondary", "alternate"} {
		sel, err = parseReadAffinity(class)
		require.NoError(t, err)
		assert.Equal(t, &affinitySelector{class: class}, sel)
	}

	sel, err = parseReadAffinity("dc:bcdc")
	require.NoError(t, err)
	assert.Equal(t, &affinitySelector{dc: "bcdc"}, sel)

	for _, bad := range []string{"primry", "dc:", "eu", "PRIMARY"} {
		_, err = parseReadAffinity(bad)
		assert.Error(t, err, bad)
	}
}

// unrawFixture runs a raw record through the same path requestMetadata uses
func unrawFixture(t *testing.T, raw string) *MetadataResponse {
	var mrr MetadataResponseRaw
	require.NoError(t, json.Unmarshal([]byte(raw), &mrr))
	m, err := mrr.unraw()
	require.NoError(t, err)
	return m
}

const fullRecord = `{
	"item_size": 123,
	"files": [{"name": "file.pdf", "size": "123"}],
	"server": "ia802907.us.archive.org",
	"d1": "ia802907.us.archive.org",
	"d2": "ia601234.us.archive.org",
	"dir": "/28/items/testitem",
	"workable_servers": ["ia802907.us.archive.org"],
	"alternate_locations": {
		"servers": [
			{"server": "dn710607.ca.archive.org", "dir": "/0/items/testitem"},
			{"server": "dn760109.eu.archive.org", "dir": "/0/items/testitem"}
		],
		"workable": [
			{"server": "dn760109.eu.archive.org", "dir": "/0/items/testitem"}
		]
	}
}`

func TestResolveCandidatesFull(t *testing.T) {
	m := unrawFixture(t, fullRecord)
	cands := resolveCandidates(m)
	require.Equal(t, []readCandidate{
		{"primary", "ia802907.us.archive.org", "/28/items/testitem", "dc8", true},
		{"secondary", "ia601234.us.archive.org", "/28/items/testitem", "dc6", false},
		{"alternate", "dn760109.eu.archive.org", "/0/items/testitem", "eu", true},
	}, cands)
	assert.Equal(t, "https://dn760109.eu.archive.org/0/items/testitem", cands[2].url())
}

func TestResolveCandidatesSolo(t *testing.T) {
	m := unrawFixture(t, `{
		"item_size": 1, "files": [],
		"d1": "ia601234.us.archive.org", "dir": "/5/items/solo"
	}`)
	cands := resolveCandidates(m)
	require.Len(t, cands, 1)
	// workable_servers absent -> assume healthy
	assert.Equal(t, readCandidate{"primary", "ia601234.us.archive.org", "/5/items/solo", "dc6", true}, cands[0])
}

func TestResolveCandidatesWorkableEmpty(t *testing.T) {
	// no workable list -> fall back to servers, marked not workable
	m := unrawFixture(t, `{
		"item_size": 1, "files": [],
		"d1": "ia802907.us.archive.org", "dir": "/28/items/x",
		"alternate_locations": {
			"servers": [{"server": "dn710607.ca.archive.org", "dir": "/0/items/x"}]
		}
	}`)
	cands := resolveCandidates(m)
	require.Len(t, cands, 2)
	assert.Equal(t, readCandidate{"alternate", "dn710607.ca.archive.org", "/0/items/x", "bcdc", false}, cands[1])
}

func TestResolveCandidatesNoLocation(t *testing.T) {
	// dark/odd records resolve to nothing; fallback handles reads
	m := unrawFixture(t, `{"item_size": 1, "files": []}`)
	assert.Empty(t, resolveCandidates(m))
}

func TestUnrawLenientLocation(t *testing.T) {
	// wrong shapes in the undocumented fields must not break parsing
	m := unrawFixture(t, `{
		"item_size": 1,
		"files": [{"name": "file.pdf", "size": "1"}],
		"d1": 42,
		"workable_servers": "nope",
		"alternate_locations": []
	}`)
	require.Len(t, m.Files, 1)
	assert.Equal(t, "", m.D1)
	assert.Empty(t, m.WorkableServers)
	assert.Empty(t, resolveCandidates(m))
}

func TestFilterByAffinity(t *testing.T) {
	m := unrawFixture(t, fullRecord)
	cands := resolveCandidates(m)

	assert.Nil(t, filterByAffinity(cands, nil))

	sel := func(s string) *affinitySelector {
		p, err := parseReadAffinity(s)
		require.NoError(t, err)
		return p
	}

	got := filterByAffinity(cands, sel("primary"))
	require.Len(t, got, 1)
	assert.Equal(t, "ia802907.us.archive.org", got[0].Server)

	got = filterByAffinity(cands, sel("secondary"))
	require.Len(t, got, 1)
	assert.Equal(t, "ia601234.us.archive.org", got[0].Server)

	got = filterByAffinity(cands, sel("alternate"))
	require.Len(t, got, 1)
	assert.Equal(t, "dn760109.eu.archive.org", got[0].Server)

	got = filterByAffinity(cands, sel("dc:eu"))
	require.Len(t, got, 1)
	assert.Equal(t, "dn760109.eu.archive.org", got[0].Server)

	assert.Empty(t, filterByAffinity(cands, sel("dc:uvic")))
}

func TestFilterByAffinityWorkableFirst(t *testing.T) {
	// secondary is the workable one; it must sort ahead of primary
	m := unrawFixture(t, `{
		"item_size": 1, "files": [],
		"d1": "ia802907.us.archive.org",
		"d2": "ia902907.us.archive.org",
		"dir": "/28/items/x",
		"workable_servers": ["ia902907.us.archive.org"]
	}`)
	got := filterByAffinity(resolveCandidates(m), &affinitySelector{dc: "dc8"})
	require.Len(t, got, 2)
	assert.Equal(t, "secondary", got[0].Class)
	assert.Equal(t, "primary", got[1].Class)
}
