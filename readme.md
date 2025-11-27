# nbt

standalone nbt library borrowed from gophertunnel

nothing much interesting to see here

it's just two extra feature included

// treats []byte/int32/int64 as array tag
Other []byte `nbt:",array"`

// catches all extra fields
Extra map[string]any `nbt:"*"`
