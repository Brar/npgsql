using System;

namespace Npgsql.Replication.Logical.Protocol
{
    /// <summary>
    /// Represents the data transmitted for a tuple in a Logical Replication Protocol message
    /// </summary>
    public readonly struct TupleData
    {
        internal TupleData(TupleDataKind kind)
        {
            if (kind != TupleDataKind.Null && kind != TupleDataKind.UnchangedToastedValue)
                throw new ArgumentException(
                    $"Invalid tuple data type {kind}. " +
                    $"Use {nameof(TupleDataKind.Null)} or {nameof(TupleDataKind.UnchangedToastedValue)}" +
                    " or specify an actual text value",
                    nameof(kind));

            Kind = kind;
            Value = null;
        }

        internal TupleData(string value)
        {
            Value = value ?? throw new ArgumentNullException(nameof(value));
            Kind = TupleDataKind.TextValue;
        }

        /// <summary>
        /// The kind of data in the tuple
        /// </summary>
        public TupleDataKind Kind { get; }

        /// <summary>
        /// The value of the tuple, in text format, if <see cref="Kind" /> is <see cref="TupleDataKind.TextValue"/>.
        /// Otherwise <see langword="null" />.
        /// </summary>
        public string? Value { get; }

        /// <inheritdoc cref="ValueType.GetHashCode"/>
        public override int GetHashCode() => Value == null ? (int)Kind : Value.GetHashCode();
    }
}
