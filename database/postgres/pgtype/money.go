package pgtype

// AppendMoney appends a money value: an int64 count of the currency's
// smallest unit, as the server stores and sends it (cash_send).
//
// What that unit is depends on the server's lc_monetary setting: two
// fractional digits under C and most locales, so 1234 is 12.34, none under
// a locale whose currency has no minor unit. The binary form does not carry
// it, and this package does not read it: the scale is the caller's, from
// SHOW lc_monetary or the application's own knowledge of its currency. A
// value read under one lc_monetary and written under another changes amount
// silently, which is the server's own behaviour. Where the scale must not
// depend on a setting, cast in the query — money::numeric reads as a
// [Numeric] at the scale the server applies, numeric::money writes back.
func AppendMoney(dst []byte, units int64) []byte { return AppendInt8(dst, units) }

// DecodeMoney decodes a money value to its count of the currency's smallest
// unit; see [AppendMoney] for the scale.
func DecodeMoney(b []byte) (int64, error) { return DecodeInt8(b) }
