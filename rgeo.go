package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RGeo is a distributed geospatial index (Redis GEO / sorted-set backed).
// Members are codec-encoded (Redisson JsonJacksonCodec interop); searches
// use GEOSEARCH (GEORADIUS is removed in Redis 8).
type RGeo struct {
	rObject
}

func newRGeo(c *Client, name string) *RGeo {
	return &RGeo{rObject{c: c, name: name}}
}

func (g *RGeo) enc(member any) (string, error) { return g.c.codec.Encode(member) }

// Add stores a member at lon/lat. Returns true when newly added.
func (g *RGeo) Add(ctx context.Context, longitude, latitude float64, member any) (bool, error) {
	enc, err := g.enc(member)
	if err != nil {
		return false, err
	}
	n, err := g.rc().GeoAdd(ctx, g.name, &redis.GeoLocation{
		Longitude: longitude, Latitude: latitude, Name: enc,
	}).Result()
	return n > 0, err
}

// TryAdd stores a member only when absent (GEOADD NX, atomic via Lua —
// go-redis does not expose the NX flag).
var geoAddNXScript = redis.NewScript(`
local s = redis.call('zscore', KEYS[1], ARGV[3])
if s then return 0 end
redis.call('geoadd', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
return 1
`)

func (g *RGeo) TryAdd(ctx context.Context, longitude, latitude float64, member any) (bool, error) {
	enc, err := g.enc(member)
	if err != nil {
		return false, err
	}
	n, err := geoAddNXScript.Run(ctx, g.rc(), []string{g.name},
		formatFloat(longitude), formatFloat(latitude), enc).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Dist returns the distance between two members (-1 when either missing).
func (g *RGeo) Dist(ctx context.Context, member1, member2 any, unit string) (float64, error) {
	e1, err := g.enc(member1)
	if err != nil {
		return 0, err
	}
	e2, err := g.enc(member2)
	if err != nil {
		return 0, err
	}
	d, err := g.rc().GeoDist(ctx, g.name, e1, e2, unit).Result()
	if err == redis.Nil {
		return -1, nil
	}
	return d, err
}

// Pos returns [lon, lat] of a member (nil when missing).
func (g *RGeo) Pos(ctx context.Context, member any) ([]float64, error) {
	enc, err := g.enc(member)
	if err != nil {
		return nil, err
	}
	pos, err := g.rc().GeoPos(ctx, g.name, enc).Result()
	if err != nil || len(pos) == 0 || pos[0] == nil {
		return nil, err
	}
	return []float64{pos[0].Longitude, pos[0].Latitude}, nil
}

// Hash returns the geohash of a member ("" when missing).
func (g *RGeo) Hash(ctx context.Context, member any) (string, error) {
	enc, err := g.enc(member)
	if err != nil {
		return "", err
	}
	hashes, err := g.rc().GeoHash(ctx, g.name, enc).Result()
	if err != nil || len(hashes) == 0 {
		return "", err
	}
	return hashes[0], nil
}

// GeoEntry is one search result.
type GeoEntry struct {
	Member    any
	Lon, Lat  float64
	Dist      float64
	HasDist   bool
}

// Search returns members within radius of lon/lat (GEOSEARCH), optionally
// with distances sorted ascending. WITHCOORD is always requested (go-redis
// cannot parse the reply shape without any WITH* flag).
func (g *RGeo) Search(ctx context.Context, longitude, latitude, radius float64, unit string,
	withDist, asc bool) ([]GeoEntry, error) {
	q := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  longitude,
			Latitude:   latitude,
			Radius:     radius,
			RadiusUnit: unit,
		},
		WithCoord: true,
		WithDist:  withDist,
	}
	if asc {
		q.Sort = "ASC"
	}
	locs, err := g.rc().GeoSearchLocation(ctx, g.name, q).Result()
	if err != nil {
		return nil, err
	}
	return g.toEntriesLoc(locs), nil
}

// SearchByMember returns members within radius of an existing member.
func (g *RGeo) SearchByMember(ctx context.Context, member any, radius float64, unit string,
	withDist, asc bool) ([]GeoEntry, error) {
	enc, err := g.enc(member)
	if err != nil {
		return nil, err
	}
	q := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Member:     enc,
			Radius:     radius,
			RadiusUnit: unit,
		},
		WithCoord: true,
		WithDist:  withDist,
	}
	if asc {
		q.Sort = "ASC"
	}
	locs, err := g.rc().GeoSearchLocation(ctx, g.name, q).Result()
	if err != nil {
		return nil, err
	}
	return g.toEntriesLoc(locs), nil
}

func (g *RGeo) toEntriesLoc(locs []redis.GeoLocation) []GeoEntry {
	out := make([]GeoEntry, 0, len(locs))
	for _, l := range locs {
		m, _ := g.c.codec.Decode(l.Name)
		out = append(out, GeoEntry{Member: m, Lon: l.Longitude, Lat: l.Latitude, Dist: l.Dist, HasDist: true})
	}
	return out
}

// Size returns the number of members.
func (g *RGeo) Size(ctx context.Context) (int64, error) {
	return g.rc().ZCard(ctx, g.name).Result()
}

// Remove deletes a member.
func (g *RGeo) Remove(ctx context.Context, member any) error {
	enc, err := g.enc(member)
	if err != nil {
		return err
	}
	return g.rc().ZRem(ctx, g.name, enc).Err()
}
