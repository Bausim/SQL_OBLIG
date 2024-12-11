using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBData.Models
{
    public class Person
    {
        public string NConst { get; set; }
        public string? PrimaryName { get; set; }
        public int? BirthYear { get; set; }
        public int? DeathYear { get; set; }
        public string? PrimaryProfession { get; set; }
        public string? KnownForTitles { get; set; }

    }
}
