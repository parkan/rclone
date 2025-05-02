package policy

import (
	"context"
	"math"
	"math/rand"

	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
)

func init() {
	registerPolicy("eplno", &EpLno{})
}

// EpLno stands for existing path, least number of objects
// Of all the candidates on which the path exists choose the one with the least number of objects
// If multiple candidates have the same number of objects, one is chosen randomly.
type EpLno struct {
	EpAll
}

func (p *EpLno) lno(upstreams []*upstream.Fs) (*upstream.Fs, error) {
	// First shuffle the list to randomize the selection order
	// This ensures that among backends with equal number of objects, one is chosen randomly
	rand.Shuffle(len(upstreams), func(i, j int) {
		upstreams[i], upstreams[j] = upstreams[j], upstreams[i]
	})

	var minNumObj int64 = math.MaxInt64
	var lnoUpstream *upstream.Fs
	for _, u := range upstreams {
		numObj, err := u.GetNumObjects()
		if err != nil {
			fs.LogPrintf(fs.LogLevelNotice, nil,
				"Number of Objects is not supported for upstream %s, treating as 0", u.Name())
		}
		if minNumObj > numObj {
			minNumObj = numObj
			lnoUpstream = u
		}
	}
	if lnoUpstream == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return lnoUpstream, nil
}

func (p *EpLno) lnoEntries(entries []upstream.Entry) (upstream.Entry, error) {
	// First shuffle the list to randomize the selection order
	// This ensures that among entries with equal number of objects, one is chosen randomly
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})

	var minNumObj int64 = math.MaxInt64
	var lnoEntry upstream.Entry
	for _, e := range entries {
		numObj, err := e.UpstreamFs().GetNumObjects()
		if err != nil {
			fs.LogPrintf(fs.LogLevelNotice, nil,
				"Number of Objects is not supported for upstream %s, treating as 0", e.UpstreamFs().Name())
		}
		if minNumObj > numObj {
			minNumObj = numObj
			lnoEntry = e
		}
	}
	return lnoEntry, nil
}

// Action category policy, governing the modification of files and directories
func (p *EpLno) Action(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error) {
	upstreams, err := p.EpAll.Action(ctx, upstreams, path)
	if err != nil {
		return nil, err
	}
	u, err := p.lno(upstreams)
	return []*upstream.Fs{u}, err
}

// ActionEntries is ACTION category policy but receiving a set of candidate entries
func (p *EpLno) ActionEntries(entries ...upstream.Entry) ([]upstream.Entry, error) {
	entries, err := p.EpAll.ActionEntries(entries...)
	if err != nil {
		return nil, err
	}
	e, err := p.lnoEntries(entries)
	return []upstream.Entry{e}, err
}

// Create category policy, governing the creation of files and directories
func (p *EpLno) Create(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error) {
	upstreams, err := p.EpAll.Create(ctx, upstreams, path)
	if err != nil {
		return nil, err
	}
	u, err := p.lno(upstreams)
	return []*upstream.Fs{u}, err
}

// CreateEntries is CREATE category policy but receiving a set of candidate entries
func (p *EpLno) CreateEntries(entries ...upstream.Entry) ([]upstream.Entry, error) {
	entries, err := p.EpAll.CreateEntries(entries...)
	if err != nil {
		return nil, err
	}
	e, err := p.lnoEntries(entries)
	return []upstream.Entry{e}, err
}

// Search category policy, governing the access to files and directories
func (p *EpLno) Search(ctx context.Context, upstreams []*upstream.Fs, path string) (*upstream.Fs, error) {
	if len(upstreams) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	upstreams, err := p.epall(ctx, upstreams, path)
	if err != nil {
		return nil, err
	}
	return p.lno(upstreams)
}

// SearchEntries is SEARCH category policy but receiving a set of candidate entries
func (p *EpLno) SearchEntries(entries ...upstream.Entry) (upstream.Entry, error) {
	if len(entries) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	return p.lnoEntries(entries)
}
