using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBData
{
    public static class AddMovieOrPerson
    {
       

        public static void AddMovie(string titleType, string primaryTitle, string originalTitle, bool isAdult, int startYear, int? endYear, int? runtimeMinutes, SqlConnection conn)
        {

                using (SqlCommand cmd = new SqlCommand("AddMovie", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.AddWithValue("@titleType", titleType);
                    cmd.Parameters.AddWithValue("@primaryTitle", primaryTitle);
                    cmd.Parameters.AddWithValue("@originalTitle", originalTitle);
                    cmd.Parameters.AddWithValue("@isAdult", isAdult);
                    cmd.Parameters.AddWithValue("@startYear", startYear);
                    cmd.Parameters.AddWithValue("@endYear", (object)endYear ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@runtimeMinutes", (object)runtimeMinutes ?? DBNull.Value);

                    
                    cmd.ExecuteNonQuery();
                    
                }
            
        }
        public static void AddPerson(string primaryName, int birthYear, int? deathYear, SqlConnection conn)
        {
            
                using (SqlCommand cmd = new SqlCommand("AddPerson", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.AddWithValue("@primaryName", primaryName);
                    cmd.Parameters.AddWithValue("@birthYear", birthYear);
                    cmd.Parameters.AddWithValue("@deathYear", (object)deathYear ?? DBNull.Value);

                    
                    cmd.ExecuteNonQuery();
                }
            
        }

       

        public static void UpdateMovie(string tconst, SqlConnection conn)
        {
            SqlConnection SqlConn = conn; 
            // Prompting user for each field; pressing Enter will skip the update for that field
            Console.WriteLine("Updating movie information. Press Enter to keep the existing value.");

            // Title Type
            Console.Write("Enter new Title Type: ");
            string titleType = Console.ReadLine();
            titleType = string.IsNullOrWhiteSpace(titleType) ? null : titleType;

            // Primary Title
            Console.Write("Enter new Primary Title: ");
            string primaryTitle = Console.ReadLine();
            primaryTitle = string.IsNullOrWhiteSpace(primaryTitle) ? null : primaryTitle;

            // Original Title
            Console.Write("Enter new Original Title: ");
            string originalTitle = Console.ReadLine();
            originalTitle = string.IsNullOrWhiteSpace(originalTitle) ? null : originalTitle;

            // Is Adult
            Console.Write("Is Adult (0 for No, 1 for Yes, press Enter to skip): ");
            string isAdultInput = Console.ReadLine();
            bool? isAdult = string.IsNullOrWhiteSpace(isAdultInput) ? (bool?)null : isAdultInput == "1";

            // Start Year
            Console.Write("Enter new Start Year: ");
            string startYearInput = Console.ReadLine();
            int? startYear = string.IsNullOrWhiteSpace(startYearInput) ? (int?)null : int.Parse(startYearInput);

            // End Year
            Console.Write("Enter new End Year: ");
            string endYearInput = Console.ReadLine();
            int? endYear = string.IsNullOrWhiteSpace(endYearInput) ? (int?)null : int.Parse(endYearInput);

            // Runtime Minutes
            Console.Write("Enter new Runtime Minutes: ");
            string runtimeMinutesInput = Console.ReadLine();
            int? runtimeMinutes = string.IsNullOrWhiteSpace(runtimeMinutesInput) ? (int?)null : int.Parse(runtimeMinutesInput);

            // Call the method to execute the update
            ExecuteUpdateMovie(conn, tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes);
        }

        private static void ExecuteUpdateMovie(SqlConnection conn, string tconst, string titleType = null, string primaryTitle = null, string originalTitle = null, bool? isAdult = null, int? startYear = null, int? endYear = null, int? runtimeMinutes = null)
        {
           
                using (SqlCommand cmd = new SqlCommand("UpdateMovie", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.AddWithValue("@tconst", tconst);
                    cmd.Parameters.AddWithValue("@titleType", (object)titleType ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@primaryTitle", (object)primaryTitle ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@originalTitle", (object)originalTitle ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@isAdult", (object)isAdult ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@startYear", (object)startYear ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@endYear", (object)endYear ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@runtimeMinutes", (object)runtimeMinutes ?? DBNull.Value);

                    
                    cmd.ExecuteNonQuery();
                }
            
        }


        public static void DeleteMovie(string tconst, SqlConnection conn)
        {
            
                using (SqlCommand cmd = new SqlCommand("DeleteMovie", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.AddWithValue("@tconst", tconst);

                    
                    cmd.ExecuteNonQuery();
                }
            
        }



    }


}
