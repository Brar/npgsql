using System.IO;

namespace Npgsql.Replication
{
    /// <summary>
    /// Contains the timeline history file for a timeline.
    /// </summary>
    public readonly struct NpgsqlTimelineHistoryFile
    {
        internal NpgsqlTimelineHistoryFile(string filename, Stream content)
        {
            Filename = filename;
            Content = content;
        }

        /// <summary>
        /// File name of the timeline history file, e.g., 00000002.history.
        /// </summary>
        public string Filename { get; }

        /// <summary>
        /// Contents of the timeline history file.
        /// </summary>
        public Stream Content { get; }
    }
}
