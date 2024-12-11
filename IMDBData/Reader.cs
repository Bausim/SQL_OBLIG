using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBData
{
    public class Reader
    {
        private string ConnString = "server=localhost,1433;database=imdbDatabase;user id=User;password=fiskmedkiks22;TrustServerCertificate=true";
        public void SearchForMovieByTitle(string title)
        {
            using (SqlConnection sqlConn = new SqlConnection(ConnString))
                try
                {
                    sqlConn.Open();

                    using (SqlCommand cmd = new SqlCommand("searchByTitle", sqlConn))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@Title", title));

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    string? primaryTitle = reader["primaryTitle"].ToString();
                                    string? year = reader["startYear"].ToString();
                                    // add more fields possibly?

                                    Console.WriteLine($"Title: {primaryTitle}, Year: {year}");
                                }
                            }
                            else
                            {
                                Console.WriteLine("No movies found with the title.");
                            }
                        }
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("an SQL error occured: " + ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("an error occured: " + ex.Message);
                }
        }

        public void SearchByName(string name)
        {
            using (SqlConnection sqlConn = new SqlConnection(ConnString))
                try
                {
                    sqlConn.Open();

                    using (SqlCommand cmd = new SqlCommand("searchByName", sqlConn))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@Name", name));

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    string? primaryName = reader["primaryName"].ToString();
                                    // add more fields possibly?

                                    Console.WriteLine($"Name: {primaryName}");
                                }
                            }
                            else
                            {
                                Console.WriteLine("No persons found with the name.");
                            }
                        }
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("an SQL error occured: " + ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("an error occured: " + ex.Message);
                }
        }
        // temporary place for the addMovieToDatabase method until it is moved to the correct file
        public void addMovieToDatabase(string primaryTitle, string originalTitle, int startYear, int runtimeMinutes)
        {
            using (SqlConnection sqlConn = new SqlConnection(ConnString))
                try
                {
                    sqlConn.Open();

                    using (SqlCommand cmd = new SqlCommand("addMovie", sqlConn))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@primaryTitle", primaryTitle));
                        cmd.Parameters.Add(new SqlParameter("@originalTitle", originalTitle));
                        cmd.Parameters.Add(new SqlParameter("@startYear", startYear));
                        cmd.Parameters.Add(new SqlParameter("@runtimeMinutes", runtimeMinutes));

                        cmd.ExecuteNonQuery();
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("an SQL error occured: " + ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("an error occured: " + ex.Message);
                }
        }
        
        // temporary place for the addPersonToDatabase method until it is moved to the correct file
        public void addPersonToDatabase(string actorName, int birthYear)
        {
            using (SqlConnection sqlConn = new SqlConnection(ConnString))
                try
                {
                    sqlConn.Open();

                    using (SqlCommand cmd = new SqlCommand("addPerson", sqlConn))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;

                        cmd.Parameters.Add(new SqlParameter("@Name", actorName));
                        cmd.Parameters.Add(new SqlParameter("@BirthYear", birthYear));

                        cmd.ExecuteNonQuery();
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("an SQL error occured: " + ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("an error occured: " + ex.Message);
                }
        }

        // how to make this one function ?
        public void UpdateMovie(string movieId, string primaryTitle, string originalTitle, int startYear, int runtimeMinutes)
        {
            using (SqlConnection sqlConn = new SqlConnection(ConnString))
                try
                {
                    sqlConn.Open();

                    using (SqlCommand cmd = new SqlCommand("updateMovie", sqlConn))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        // How does user get movieId?
                        cmd.Parameters.Add(new SqlParameter("@movieId", movieId));

                        // user inputs will change the values of these parameters.
                        cmd.Parameters.Add(new SqlParameter("@primaryTitle", primaryTitle));
                        cmd.Parameters.Add(new SqlParameter("@originalTitle", originalTitle));
                        cmd.Parameters.Add(new SqlParameter("@startYear", startYear));
                        cmd.Parameters.Add(new SqlParameter("@runtimeMinutes", runtimeMinutes));

                        cmd.ExecuteNonQuery();
                    }
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("an SQL error occured: " + ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("an error occured: " + ex.Message);
                }
        }
    }
}
