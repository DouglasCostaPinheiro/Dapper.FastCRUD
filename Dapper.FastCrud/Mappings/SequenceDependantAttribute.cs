using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.FastCrud.Mappings
{
    /// <summary>
    /// Denotes a column that is dependant on a Oracle sequence
    /// Properties marked with this attributes will be ignored on INSERT but refreshed from the database.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class SequenceDependantAttribute : Attribute
    {
        public string SequenceName { get; private set; }
        public bool SequenceUpdatedByTrigger { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sequenceName">The name of the sequence on the database</param>
        /// <param name="sequenceUpdatedByTrigger">True if a trigger is used to automatically increase the value on insert, false otherwise</param>
        public SequenceDependantAttribute(string sequenceName, bool sequenceUpdatedByTrigger = false)
        {
            this.SequenceName = sequenceName;
            this.SequenceUpdatedByTrigger = sequenceUpdatedByTrigger;
        }
    }
}
