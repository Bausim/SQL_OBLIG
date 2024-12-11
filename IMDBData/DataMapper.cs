using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Collections.Generic;

namespace IMDBData
{
    public class DataMapper
    {
        private Dictionary<int, string> genreDictionary;
        private Dictionary<int, string> professionDictionary;


        public DataMapper(SqlConnection sqlconnection)
        {
            genreDictionary = LoadGenreMappings(sqlconnection);
            professionDictionary = LoadProfessionMappings(sqlconnection);
        }

        private Dictionary<int, string> LoadGenreMappings(SqlConnection sqlconnection)
        {
            var genres = new Dictionary<int, string>();
            string genreQuery = "SELECT genreID, genre FROM Genres";

            using (SqlCommand cmd = new SqlCommand(genreQuery, sqlconnection))
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    genres.Add(reader.GetInt32(0), reader.GetString(1));
                }
            }
            return genres;

        }

        private Dictionary<int, string> LoadProfessionMappings(SqlConnection sqlconnection)
        {
            var professions = new Dictionary<int, string>();
            string professionQuery = "SELECT ProfessionID, Profession FROM Profession";

            using (SqlCommand cmd = new SqlCommand(professionQuery, sqlconnection))
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    professions.Add(reader.GetInt32(0), reader.GetString(1));
                }
            }
            return professions;
        }

        public string GetGenreName(int genreID) => genreDictionary.TryGetValue(genreID, out var name) ? name : "Unknown";
        public string GetProfessionName(int professionID) => professionDictionary.TryGetValue(professionID, out var name) ? name : "Unknown";
    }
}

