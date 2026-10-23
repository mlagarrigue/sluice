// Package pgtype converts Go values to PostgreSQL's binary representation and
// back, with no connection involved.
//
// Each type has an Append function, which writes a value at the end of a
// buffer the caller owns, and a Decode function, which reads one from the
// bytes the server sent; the OID constants name the types a Parse message
// carries and a RowDescription reports. Arrays have their own pair, and a
// column that may hold NULL decodes into a [Null] of its type. [Numeric],
// [Interval], [Bits], [TimeOfDay] and [TimeTZ] hold what no Go type holds
// exactly.
//
// The package is the codec of github.com/mlagarrigue/sluice/database/postgres
// and imports nothing of it: building a COPY payload, testing an encoding or
// writing a code generator needs no connection. What needs one stays there —
// the Scan functions read a batch of rows, and the COPY row framing belongs
// to the copy protocol.
//
// A Decode function that returns [ErrCodec] has read bytes that do not match
// the type it was told they were, which means the stream and its description
// disagree, never that the data was merely surprising.
package pgtype
