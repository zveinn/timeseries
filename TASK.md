I want you to build me a small system to store timeseries data in golang. This system will store timeseries data directly on the filesystem in a namespace type manner (see more below). The data will be cbor encoded.

STORAGE PATH PATTERN:
 - year/month/day/hour/minute.cbor

API:
 - timeseries.Store[T any](date time.Time, data T) error
 - timeseries.Get[T any](from time.Time, to time.Time) (d []*T, err error)
 - timeseries.Find[T any](from time.Time, to time.Time, func()) error
   - func() is called for every object we iterate over
 - timeseries.Delete(from time.Time, to timeTime) error
 - timeseries.Init[T any](opt *Options) (client *Client)

NOTES:
 - use generics to define the data type
 - every call to Init should create a new client
 - do md5 validation post write
 - each item in the minute.cbor file should be line by line
 - we have already made some basic structs and the basic Init method
 - use cbor stream encoder/decoder when reading/writing data from disk
 - DO NOT MAKE CODE COMMENTS

CACHE:
when calling Init, we need to walk the entire dir structure and cache the 'year/month/day/hour/minute' timestamp to a full directory path using a slice (the slice has already been implemented inside the client)



