package policy

import (
	"context"
	"errors"
	"math"
	"math/rand"

	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
)

func init() {
	registerPolicy("eplfs", &EpLfs{})
}

// EpLfs stands for existing path, least free space
// Of all the candidates on which the path exists choose the one with the least free space.
// If multiple candidates have the same amount of free space, one is chosen randomly.
type EpLfs struct {
	EpAll
}

var errNoUpstreamsFound = errors.New("no upstreams found with more than min_free_space space spare")

func (p *EpLfs) lfs(upstreams []*upstream.Fs) (*upstream.Fs, error) {
	// First shuffle the list to randomize the selection order
	// This ensures that among backends with equal free space, one is chosen randomly
	rand.Shuffle(len(upstreams), func(i, j int) {
		upstreams[i], upstreams[j] = upstreams[j], upstreams[i]
	})

	var minFreeSpace int64 = math.MaxInt64
	var lfsupstream *upstream.Fs
	for _, u := range upstreams {
		space, err := u.GetFreeSpace()
		if err != nil {
			fs.LogPrintf(fs.LogLevelNotice, nil,
				"Free Space is not supported for upstream %s, treating as infinite", u.Name())
		}
		if space < minFreeSpace && space > int64(u.Opt.MinFreeSpace) {
			minFreeSpace = space
			lfsupstream = u
		}
	}
	if lfsupstream == nil {
		return nil, errNoUpstreamsFound
	}
	return lfsupstream, nil
}

func (p *EpLfs) lfsEntries(entries []upstream.Entry) (upstream.Entry, error) {
	// First shuffle the list to randomize the selection order
	// This ensures that among entries with equal free space, one is chosen randomly
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})

	var minFreeSpace int64 = math.MaxInt64
	var lfsEntry upstream.Entry
	for _, e := range entries {
		u := e.UpstreamFs()
		space, err := u.GetFreeSpace()
		if err != nil {
			fs.LogPrintf(fs.LogLevelNotice, nil,
				"Free Space is not supported for upstream %s, treating as infinite", u.Name())
		}
		if space < minFreeSpace && space > int64(u.Opt.MinFreeSpace) {
			minFreeSpace = space
			lfsEntry = e
		}
	}
	if lfsEntry == nil {
		return nil, errNoUpstreamsFound
	}
	return lfsEntry, nil
}

// Action category policy, governing the modification of files and directories
func (p *EpLfs) Action(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error) {
	upstreams, err := p.EpAll.Action(ctx, upstreams, path)
	if err != nil {
		return nil, err
	}
	u, err := p.lfs(upstreams)
	return []*upstream.Fs{u}, err
}

// ActionEntries is ACTION category policy but receiving a set of candidate entries
func (p *EpLfs) ActionEntries(entries ...upstream.Entry) ([]upstream.Entry, error) {
	entries, err := p.EpAll.ActionEntries(entries...)
	if err != nil {
		return nil, err
	}
	e, err := p.lfsEntries(entries)
	return []upstream.Entry{e}, err
}

// Create category policy, governing the creation of files and directories
func (p *EpLfs) Create(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error) {
	upstreams, err := p.EpAll.Create(ctx, upstreams, path)
	if err != nil {
		return nil, err
	}
	u, err := p.lfs(upstreams)
	return []*upstream.Fs{u}, err
}

// CreateEntries is CREATE category policy but receiving a set of candidate entries
func (p *EpLfs) CreateEntries(entries ...upstream.Entry) ([]upstream.Entry, error) {
	entries, err := p.EpAll.CreateEntries(entries...)
	if err != nil {
		return nil, err
	}
	e, err := p.lfsEntries(entries)
	return []upstream.Entry{e}, err
}

// Search category policy, governing the access to files and directories
func (p *EpLfs) Search(ctx context.Context, upstreams []*upstream.Fs, path string) (*upstream.Fs, error) {
	if len(upstreams) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	upstreams, err := p.epall(ctx, upstreams, path)
	if err != nil {
		return nil, err
	}
	return p.lfs(upstreams)
}

// SearchEntries is SEARCH category policy but receiving a set of candidate entries
func (p *EpLfs) SearchEntries(entries ...upstream.Entry) (upstream.Entry, error) {
	if len(entries) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	return p.lfsEntries(entries)
}
