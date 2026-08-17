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

// GeoLocation describes one member and its longitude/latitude.
type GeoLocation struct {
	Lon, Lat float64
	Member   any
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

// AddMany stores all locations and returns the number of newly added members.
func (g *RGeo) AddMany(ctx context.Context, locations []GeoLocation) (int64, error) {
	if len(locations) == 0 {
		return 0, nil
	}
	values := make([]*redis.GeoLocation, len(locations))
	for i, location := range locations {
		enc, err := g.enc(location.Member)
		if err != nil {
			return 0, err
		}
		values[i] = &redis.GeoLocation{
			Longitude: location.Lon,
			Latitude:  location.Lat,
			Name:      enc,
		}
	}
	return g.rc().GeoAdd(ctx, g.name, values...).Result()
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

var geoAddXXScript = redis.NewScript(`
if redis.call('zscore', KEYS[1], ARGV[3]) == false then
    return 0
end
redis.call('geoadd', KEYS[1], 'XX', ARGV[1], ARGV[2], ARGV[3])
return 1
`)

// AddIfExists updates a member only when it already exists (GEOADD XX).
func (g *RGeo) AddIfExists(ctx context.Context, longitude, latitude float64, member any) (bool, error) {
	enc, err := g.enc(member)
	if err != nil {
		return false, err
	}
	n, err := geoAddXXScript.Run(ctx, g.rc(), []string{g.name},
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

// PosMany returns positions aligned with members; missing members yield nil.
func (g *RGeo) PosMany(ctx context.Context, members ...any) ([][]float64, error) {
	if len(members) == 0 {
		return [][]float64{}, nil
	}
	encoded := make([]string, len(members))
	for i, member := range members {
		enc, err := g.enc(member)
		if err != nil {
			return nil, err
		}
		encoded[i] = enc
	}
	positions, err := g.rc().GeoPos(ctx, g.name, encoded...).Result()
	if err != nil {
		return nil, err
	}
	out := make([][]float64, len(positions))
	for i, position := range positions {
		if position != nil {
			out[i] = []float64{position.Longitude, position.Latitude}
		}
	}
	return out, nil
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

// HashMany returns geohashes aligned with members; missing members yield "".
func (g *RGeo) HashMany(ctx context.Context, members ...any) ([]string, error) {
	if len(members) == 0 {
		return []string{}, nil
	}
	encoded := make([]string, len(members))
	for i, member := range members {
		enc, err := g.enc(member)
		if err != nil {
			return nil, err
		}
		encoded[i] = enc
	}
	return g.rc().GeoHash(ctx, g.name, encoded...).Result()
}

// GeoEntry is one search result.
type GeoEntry struct {
	Member   any
	Lon, Lat float64
	Dist     float64
	HasDist  bool
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

// StoreSearchTo stores members within radius in dest and returns the count.
func (g *RGeo) StoreSearchTo(ctx context.Context, dest string, longitude, latitude, radius float64,
	unit string, count int) (int64, error) {
	q := &redis.GeoSearchStoreQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  longitude,
			Latitude:   latitude,
			Radius:     radius,
			RadiusUnit: unit,
			Count:      count,
		},
	}
	return g.rc().GeoSearchStore(ctx, g.name, dest, q).Result()
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
