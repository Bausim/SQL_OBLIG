using System.Data.SqlClient;
using System.Data;

public class SearchService
{
    private SqlConnection sqlConnection;

    public SearchService(SqlConnection sqlConnection)
    {
        this.sqlConnection = sqlConnection;
    }

    public void SearchTitle(string titleSearchTerm)
    {
        using (SqlCommand cmd = new SqlCommand("SearchTitles", sqlConnection))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Titles", titleSearchTerm);

            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                Console.WriteLine("Results for Title Search:");
                Console.WriteLine("Title | Genres | Directors | Writers");

                while (reader.Read())
                {
                    string title = reader["Title"].ToString();
                    string genres = reader["Genres"].ToString();
                    string directors = reader["Directors"].ToString();
                    string writers = reader["Writers"].ToString();

                    Console.WriteLine($"{title} | {genres} | {directors} | {writers}");
                }
            }
        }
    }

    public void SearchPerson(string personSearchTerm)
    {
        using (SqlCommand cmd = new SqlCommand("SearchPersons", sqlConnection))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Name", personSearchTerm);

            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                Console.WriteLine("Results for Person Search:");
                Console.WriteLine("Name | Professions | Known For Titles");

                while (reader.Read())
                {
                    string name = reader["PersonName"].ToString();
                    string professions = reader["Professions"].ToString();
                    string knownForTitles = reader["KnownForTitles"].ToString();

                    Console.WriteLine($"{name} | {professions} | {knownForTitles}");
                }
            }
        }
    }
}