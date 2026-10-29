package pgtype

// Type OIDs of the scalar types this package encodes, in OID order. They are
// the numbers a Parse message carries and a RowDescription reports, and
// naming them is what keeps a switch on 1184 from being unreadable.
const (
	OIDBool        uint32 = 16
	OIDBytea       uint32 = 17
	OIDName        uint32 = 19
	OIDInt8        uint32 = 20
	OIDInt2        uint32 = 21
	OIDInt4        uint32 = 23
	OIDText        uint32 = 25
	OIDJSON        uint32 = 114 // not interchangeable with jsonb on the wire, and mixing them is silent: see [AppendJSONB]
	OIDCIDR        uint32 = 650 // the inet layout, with every bit below the netmask zero
	OIDFloat4      uint32 = 700
	OIDFloat8      uint32 = 701
	OIDMacaddr8    uint32 = 774
	OIDMacaddr     uint32 = 829
	OIDInet        uint32 = 869  // keeps the bits below the netmask: 192.168.1.1/24 is an inet and cannot be a cidr
	OIDBPChar      uint32 = 1042 // char(n): text, blank-padded to n
	OIDVarchar     uint32 = 1043
	OIDDate        uint32 = 1082
	OIDTime        uint32 = 1083
	OIDTimestamp   uint32 = 1114
	OIDTimestampTZ uint32 = 1184
	OIDInterval    uint32 = 1186
	OIDTimeTZ      uint32 = 1266
	OIDBit         uint32 = 1560
	OIDVarbit      uint32 = 1562
	OIDNumeric     uint32 = 1700 // arbitrary-precision decimal
	OIDUUID        uint32 = 2950
	OIDJSONB       uint32 = 3802
)

// Array type OIDs, one per element type the Append…Array and Decode…Array
// functions of this package cover, in OID order.
const (
	OIDBoolArray        uint32 = 1000
	OIDByteaArray       uint32 = 1001
	OIDInt2Array        uint32 = 1005
	OIDInt4Array        uint32 = 1007
	OIDTextArray        uint32 = 1009
	OIDInt8Array        uint32 = 1016
	OIDFloat4Array      uint32 = 1021
	OIDFloat8Array      uint32 = 1022
	OIDTimestampArray   uint32 = 1115
	OIDDateArray        uint32 = 1182
	OIDTimestampTZArray uint32 = 1185
	OIDNumericArray     uint32 = 1231
	OIDUUIDArray        uint32 = 2951
)
