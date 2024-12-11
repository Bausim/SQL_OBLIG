using IMDBData.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBData
{
    // overvejer at gøre hele classen static
    public class LoadResult
    {
        public List<Title> Titles { get; set; } = new List<Title>();

        // overvej at ændre den her til HashSet i stedet for List (fuck hvor HashSets er nice)
        public List<Genre> Genres { get; set; } = new List<Genre>();
        public List<TitleGenre> TitleGenres { get; set; } = new List<TitleGenre>();

        // laver den om til HashSet for at spare på at code så vi ikke får de samme professions ind i arrayet 
        public HashSet<string> professions { get; set; } = new HashSet<string>();

        public List<Person> persons { get; set; } = new List<Person>();

        public List<PersonProfession> personProfessionn { get; set; } = new List<PersonProfession>();
        public List<TitleWriter> titleWriters { get; set; } = new List<TitleWriter>();
        public List<TitleDirector> titleDirector { get; set; } = new List<TitleDirector>();

        public List<KnownForTitles> knownForTitles { get; set; } = new List<KnownForTitles>();
        //fordig jeg tror ikke vi har alle de tconst vi mangler til KnownForTitles (skaber foreign key conflict)
        public static HashSet<string> tconstHS { get; set; } = new HashSet<String>();
        public static HashSet<string> nconstHS { get; set; } = new HashSet<string>();

        // Spørgsmål til senere om det ville være bedre at have en dictionary til KFT så vi ikke behøver at køre Person datasettet igennem igen.

        public static Dictionary<string, string> knownForTitlesDict { get; set; } = new Dictionary<string, string>(); 

        public static Dictionary<string, int> professionDict { get; set; } = new Dictionary<string, int>();

        public static Dictionary<string, int> genreIdMap = new Dictionary<string, int>();

       
    

        //public static Dictionary<string, int> writerDict { get; set; } = new Dictionary<string, int>();
        //public HashSet<string> writers { get; set; } = new HashSet<string>();
        //public HashSet<string> directors { get; set; } = new HashSet<string>();
        //public static Dictionary<string, int> directorDict { get; set; } = new Dictionary<string, int>();

        
    }
}
