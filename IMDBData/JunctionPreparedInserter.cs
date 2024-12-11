using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using IMDBData.Models;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;

namespace IMDBData
{
    public class JunctionPreparedInserter
    {
        private HashSet<string> existingNConstCache = new HashSet<string>();

        public void Insert(List<TitleGenre> titleGenres, List<PersonProfession> pp, List<KnownForTitles> knownForTitles, List<TitleDirector> titDirector, List<TitleWriter> titWriter, SqlConnection sqlConn, SqlTransaction transAction)
        {



            string TitleGenreSQL = "INSERT INTO [TitleGenres] ([tconst], [GenreID]) VALUES (@tconst, @GenreID)";
            SqlCommand titleGenreSqlComm = new SqlCommand(TitleGenreSQL, sqlConn, transAction);

            // Create parameters for tconst and genre
            SqlParameter tconstTGPar = new SqlParameter("@tconst", SqlDbType.VarChar, 50);
            SqlParameter genreIDPar = new SqlParameter("@GenreID", SqlDbType.Int);
            titleGenreSqlComm.Parameters.Add(tconstTGPar);
            titleGenreSqlComm.Parameters.Add(genreIDPar);
            titleGenreSqlComm.Prepare();

            Console.WriteLine("TitleGenre prepared..");

            foreach (var titleGenre in titleGenres)
            {
                tconstTGPar.Value = titleGenre.TConst;
                genreIDPar.Value = titleGenre.GenreID;

                titleGenreSqlComm.ExecuteNonQuery();


            }
            Console.WriteLine("TitleGenre Executed..");

            #region PersonProfession insertion

            string ppSQL = "INSERT INTO [PersonsProfessions]([ProfessionID], [nconst]) VALUES (@ProfessionID, @nconst)";
            SqlCommand ppSQLComm = new SqlCommand(ppSQL, sqlConn, transAction);

            //Params for pp
            SqlParameter professionidPar = new SqlParameter("@ProfessionID", SqlDbType.Int);
            SqlParameter nconstPar = new SqlParameter("@nconst", SqlDbType.NVarChar, 50);
            ppSQLComm.Parameters.Add(professionidPar);
            ppSQLComm.Parameters.Add(nconstPar);
            ppSQLComm.Prepare();

            Console.WriteLine("PersonsProfession prepared");

            foreach (var personProfession in pp)
            {
                professionidPar.Value = personProfession.professionID;
                nconstPar.Value = personProfession.NConst;

                ppSQLComm.ExecuteNonQuery();
            }

            Console.WriteLine("PersonProfession Executed...");
            #endregion

            #region KnownForTitle Insertion
            string KFTSQL = "INSERT INTO [KnownForTitles] ([tconst], [nconst]) VALUES (@tconst, @nconst)";
            SqlCommand KFTSQLComm = new SqlCommand(KFTSQL, sqlConn, transAction);

            SqlParameter tconstKFTPar = new SqlParameter("@tconst", SqlDbType.NVarChar, 50);
            SqlParameter nconstKFTPar = new SqlParameter("@nconst", SqlDbType.NVarChar, 50);

            // Add parameters to the command
            KFTSQLComm.Parameters.Add(tconstKFTPar);
            KFTSQLComm.Parameters.Add(nconstKFTPar);

            KFTSQLComm.Prepare();
            Console.WriteLine("KnownForTitle sql command prepared...");

            foreach (KnownForTitles kft in knownForTitles)
            {
                if (LoadResult.tconstHS.Contains(kft.tconst))
                {
                    try
                    {
                        tconstKFTPar.Value = kft.tconst;
                        nconstKFTPar.Value = kft.nconst;


                        KFTSQLComm.ExecuteNonQuery();
                    }
                    catch (SqlException ex) when (ex.Number == 2627)
                    {
                        //Console.WriteLine($"Duplicate entry for tconst {kft.tconst}, nconst {kft.nconst} - skipping insertion");
                    }
                }
                else
                {
                    //Console.WriteLine($"Skipping KnownForTitle for tconst {kft.tconst} - Not included in our Loaded Data");
                }
            }
            Console.WriteLine("KnownForTitle sql command executed...");
            #endregion
            #region Writer Insertion
            if (titWriter == null) 
            {
                Console.WriteLine("Error: titWriter list is null.");
                return;
            }

            if (sqlConn == null || transAction == null)
            {
                Console.WriteLine("Error: SQL connection or transaction is null.");
                return;
            }

            string writerSQL = "INSERT INTO [TitleWriters]([tconst], [nconst]) VALUES (@tconst, @nconst)";
            SqlCommand writerSQLComm = new SqlCommand(writerSQL, sqlConn, transAction);
            SqlParameter titPar = new SqlParameter("@tconst", SqlDbType.NVarChar, 50);
            SqlParameter perPar = new SqlParameter("@nconst", SqlDbType.NVarChar, 50);
            writerSQLComm.Parameters.Add(titPar);
            writerSQLComm.Parameters.Add(perPar);

            try
            {
                writerSQLComm.Prepare();
                Console.WriteLine("Writers SQL Command Prepared...");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to prepare SQL command: " + ex.Message);
                return;
            }

            // Et HashSet til at holde styr på de tconst, nconst par der er blevet inserted.
            var insertedTitleWriters = new HashSet<(string TConst, string NConst)>();

            foreach (TitleWriter writer in titWriter)
            {
                if (writer == null || string.IsNullOrEmpty(writer.NConst) || string.IsNullOrEmpty(writer.TConst))
                {
                    //Console.WriteLine("Error: Encountered null TitleDirector object or missing nconst.");
                    continue;
                }

                var writerKey = (writer.TConst, writer.NConst);
                if (insertedTitleWriters.Contains(writerKey))
                {
                    //Console.WriteLine($"Skipping duplicate TitleWriter entry for TConst={writer.TConst}, NConst={writer.NConst}");
                    continue;
                }


                if (!existingNConstCache.Contains(writer.NConst))
                {
                    if (CheckPersonExists(sqlConn, transAction, writer.NConst) && CheckTitleExists(sqlConn, transAction, writer.TConst))
                    {
                        existingNConstCache.Add(writer.NConst);
                    }
                    else
                    {
                        // Console.WriteLine($"Skipping non-existent Person entry for nconst {director.NConst}");
                        continue;
                    }
                }

                try
                {
                    titPar.Value = checkObjectForNull(writer.TConst);
                    perPar.Value = checkObjectForNull(writer.NConst);

                    //Console.WriteLine($"Inserting Director with TConst={titlePar.Value}, NConst={personPar.Value}");
                    writerSQLComm.ExecuteNonQuery();
                    insertedTitleWriters.Add(writerKey);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Insert failed for TConst={writer.TConst}, NConst={writer.NConst}: {ex.Message}");
                }
            }
            Console.WriteLine("Director SQL command executed..");

            #endregion

            #region Director Insertion
            if (titDirector == null)
            {
                Console.WriteLine("Error: titDirector list is null.");
                return; // Exit if null to avoid null reference issues.
            }

            if (sqlConn == null || transAction == null)
            {
                Console.WriteLine("Error: SQL connection or transaction is null.");
                return;
            }

            string directorSQL = "INSERT INTO [TitleDirectors]([tconst], [nconst]) VALUES (@tconst, @nconst)";
            SqlCommand directorSQLComm = new SqlCommand(directorSQL, sqlConn, transAction);
            SqlParameter titlePar = new SqlParameter("@tconst", SqlDbType.NVarChar, 50);
            SqlParameter personPar = new SqlParameter("@nconst", SqlDbType.NVarChar, 50);
            directorSQLComm.Parameters.Add(titlePar);
            directorSQLComm.Parameters.Add(personPar);

            try
            {
                directorSQLComm.Prepare();
                Console.WriteLine("Directors SQL Command Prepared...");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to prepare SQL command: " + ex.Message);
                return;
            }

            foreach (TitleDirector director in titDirector)
            {
                if (director == null || string.IsNullOrEmpty(director.NConst))
                {
                    //Console.WriteLine("Error: Encountered null TitleDirector object or missing nconst.");
                    continue;
                }

                if (!existingNConstCache.Contains(director.NConst))
                {
                    if (CheckPersonExists(sqlConn, transAction, director.NConst) && CheckTitleExists(sqlConn, transAction, director.TConst))
                    {
                        existingNConstCache.Add(director.NConst);
                    }
                    else
                    {
                       // Console.WriteLine($"Skipping non-existent Person entry for nconst {director.NConst}");
                        continue;
                    }
                }

                try
                {
                    titlePar.Value = checkObjectForNull(director.TConst);
                    personPar.Value = checkObjectForNull(director.NConst);

                    //Console.WriteLine($"Inserting Director with TConst={titlePar.Value}, NConst={personPar.Value}");
                    directorSQLComm.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Insert failed for TConst={director.TConst}, NConst={director.NConst}: {ex.Message}");
                }
            }
            Console.WriteLine("Director SQL command executed..");
            #endregion

        }

        public bool CheckPersonExists(SqlConnection sqlConn, SqlTransaction transAction, string nconst)
        {
            if (sqlConn == null || transAction == null || string.IsNullOrEmpty(nconst))
            {
                Console.WriteLine("CheckPersonExists encountered null or empty parameter(s).");
                return false;
            }

            using (SqlCommand cmd = new SqlCommand("CheckPersonExists", sqlConn, transAction))
            {
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add(new SqlParameter("@nconst", SqlDbType.NVarChar, 50) { Value = nconst });

                // Add output parameter
                SqlParameter existsParam = new SqlParameter("@Exists", SqlDbType.Bit)
                {
                    Direction = ParameterDirection.Output
                };
                cmd.Parameters.Add(existsParam);

                try
                {
                    cmd.ExecuteNonQuery();
                    bool exists = (bool)existsParam.Value;
                    Console.WriteLine($"CheckPersonExists result for nconst={nconst}: {exists}");
                    return exists;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in CheckPersonExists for nconst={nconst}: {ex.Message}");
                    return false;
                }
            }
        }

        public bool CheckTitleExists(SqlConnection sqlConn, SqlTransaction transAction, string tconst)
        {
            if (sqlConn == null || transAction == null || string.IsNullOrEmpty(tconst))
            {
                Console.WriteLine("CheckPersonExists encountered null or empty parameter(s).");
                return false;
            }
            using (SqlCommand cmd = new SqlCommand("CheckTitleExists", sqlConn, transAction))
            {
                cmd.CommandType = CommandType.StoredProcedure;

                cmd.Parameters.Add(new SqlParameter("@tconst", SqlDbType.NVarChar, 50) { Value = tconst });

                // Add output parameter
                SqlParameter existsParam = new SqlParameter("@Exists", SqlDbType.Bit)
                {
                    Direction = ParameterDirection.Output
                };
                cmd.Parameters.Add(existsParam);

                try
                {
                    cmd.ExecuteNonQuery();
                    bool exists = (bool)existsParam.Value;
                   // Console.WriteLine($"CheckPersonExists result for nconst={tconst}: {exists}");
                    return exists;
                }
                catch (Exception ex)
                {
                    //Console.WriteLine($"Error in CheckPersonExists for nconst={tconst}: {ex.Message}");
                    return false;
                }
            }

           
        }

        public object checkObjectForNull(Object? value)
        {
            if (value == null)
            {
                return DBNull.Value;
            }
            return value;
        }

    }
}
