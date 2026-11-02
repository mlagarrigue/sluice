package pgtype

// Type OIDs of the scalar types this package encodes, in OID order. They are
// the numbers a Parse message carries and a RowDescription reports, and
// naming them is what keeps a switch on 1184 from being unreadable.
const (
	OIDBool          uint32 = 16
	OIDBytea         uint32 = 17
	OIDChar          uint32 = 18 // "char", one byte, not char(n): see [AppendChar]
	OIDName          uint32 = 19
	OIDInt8          uint32 = 20
	OIDInt2          uint32 = 21
	OIDInt2Vector    uint32 = 22 // an int2[] numbered from 0
	OIDInt4          uint32 = 23
	OIDRegproc       uint32 = 24 // an oid on the wire, as every reg* alias
	OIDText          uint32 = 25
	OIDOID           uint32 = 26
	OIDTID           uint32 = 27
	OIDXID           uint32 = 28
	OIDCID           uint32 = 29
	OIDOIDVector     uint32 = 30  // an oid[] numbered from 0
	OIDJSON          uint32 = 114 // not interchangeable with jsonb on the wire, and mixing them is silent: see [AppendJSONB]
	OIDXML           uint32 = 142 // text on the wire, in the client encoding: see [AppendJSONPath]
	OIDPoint         uint32 = 600
	OIDLSeg          uint32 = 601
	OIDPath          uint32 = 602
	OIDBox           uint32 = 603
	OIDPolygon       uint32 = 604
	OIDLine          uint32 = 628
	OIDCIDR          uint32 = 650 // the inet layout, with every bit below the netmask zero
	OIDFloat4        uint32 = 700
	OIDFloat8        uint32 = 701
	OIDCircle        uint32 = 718
	OIDMacaddr8      uint32 = 774
	OIDMoney         uint32 = 790 // an int64 of the currency's smallest unit, scaled by lc_monetary: see [AppendMoney]
	OIDMacaddr       uint32 = 829
	OIDInet          uint32 = 869  // keeps the bits below the netmask: 192.168.1.1/24 is an inet and cannot be a cidr
	OIDBPChar        uint32 = 1042 // char(n): text, blank-padded to n
	OIDVarchar       uint32 = 1043
	OIDDate          uint32 = 1082
	OIDTime          uint32 = 1083
	OIDTimestamp     uint32 = 1114
	OIDTimestampTZ   uint32 = 1184
	OIDInterval      uint32 = 1186
	OIDTimeTZ        uint32 = 1266
	OIDBit           uint32 = 1560
	OIDVarbit        uint32 = 1562
	OIDNumeric       uint32 = 1700 // arbitrary-precision decimal
	OIDRefcursor     uint32 = 1790 // a cursor's name, text on the wire
	OIDRegprocedure  uint32 = 2202
	OIDRegoper       uint32 = 2203
	OIDRegoperator   uint32 = 2204
	OIDRegclass      uint32 = 2205
	OIDRegtype       uint32 = 2206
	OIDUUID          uint32 = 2950
	OIDTxidSnapshot  uint32 = 2970 // the pg_snapshot layout
	OIDPGLSN         uint32 = 3220
	OIDRegconfig     uint32 = 3734
	OIDRegdictionary uint32 = 3769
	OIDTSVector      uint32 = 3614
	OIDTSQuery       uint32 = 3615
	OIDJSONB         uint32 = 3802
	OIDJSONPath      uint32 = 4072
	OIDRegnamespace  uint32 = 4089
	OIDRegrole       uint32 = 4096
	OIDRegcollation  uint32 = 4191
	OIDPGSnapshot    uint32 = 5038
	OIDXID8          uint32 = 5069
	OIDOID8          uint32 = 6437 // PostgreSQL 19
	OIDRegdatabase   uint32 = 6490 // PostgreSQL 19
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
