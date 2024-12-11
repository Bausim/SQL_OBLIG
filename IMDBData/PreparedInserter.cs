using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using IMDBData.Models;
using System.Data.SqlClient;
using System.IO;

namespace IMDBData
{
    public class PreparedInserter : IInserter
    {
        // Dictionary to map genre names to their IDs for future lookup
        //Dictionary<string, int> genreIdMap = new Dictionary<string, int>();
        public void Insert(List<Title> titles, List<Person> persons, List<Genre> genres, HashSet<string> professions, SqlConnection sqlConn, SqlTransaction transAction)
        {
            Console.WriteLine("starting insertion");
            string TitleSQL = "INSERT INTO [Title]([tconst]," +
                "[primarytitle],[originaltitle],[isadult],[startyear]," +
                "[endyear],[runtimeminutes])" +
                "VALUES(@tconst," +
                "@primarytitle," +
                "@originaltitle," +
                "@isadult," +
                "@startyear," +
                "@endyear," +
                "@runtimeminutes)";



            SqlCommand TitleSqlComm = new SqlCommand(TitleSQL, sqlConn, transAction);

            SqlParameter tconstPar = new SqlParameter("@tconst",
                SqlDbType.VarChar, 50);
            TitleSqlComm.Parameters.Add(tconstPar);

            SqlParameter primaryTitlePar = new SqlParameter("@primarytitle",
                SqlDbType.VarChar, 255);
            TitleSqlComm.Parameters.Add(primaryTitlePar);

            SqlParameter originalTitlePar = new SqlParameter("@originaltitle",
                SqlDbType.VarChar, 255);
            TitleSqlComm.Parameters.Add(originalTitlePar);

            SqlParameter isAdultPar = new SqlParameter("@isadult",
                SqlDbType.Bit);
            TitleSqlComm.Parameters.Add(isAdultPar);

            SqlParameter startYearPar = new SqlParameter("@startyear",
                SqlDbType.Int);
            TitleSqlComm.Parameters.Add(startYearPar);

            SqlParameter endYearPar = new SqlParameter("@endyear",
                SqlDbType.Int);
            TitleSqlComm.Parameters.Add(endYearPar);

            SqlParameter runtimeMinutesPar = new SqlParameter("@runtimeminutes",
                SqlDbType.Int);
            TitleSqlComm.Parameters.Add(runtimeMinutesPar);

            TitleSqlComm.Prepare();
            Console.WriteLine("Title Sql prepared..");


            foreach (Title title in titles)
            {
                tconstPar.Value = title.TConst;
                primaryTitlePar.Value = checkObjectForNull(title.PrimaryTitle);
                originalTitlePar.Value = checkObjectForNull(title.OriginalTitle);
                isAdultPar.Value = title.IsAdult;
                startYearPar.Value = checkObjectForNull(title.StartYear);
                endYearPar.Value = checkObjectForNull(title.EndYear);
                runtimeMinutesPar.Value = checkObjectForNull(title.RuntimeMinutes);

                TitleSqlComm.ExecuteNonQuery();
            }
            Console.WriteLine("Title sql command executed..");

            // Insert into Genre table and retrieve GenreID
            string GenreSQL = "INSERT INTO [Genres]([genre]) OUTPUT INSERTED.genreID VALUES(@genre)";
            SqlCommand genreSqlComm = new SqlCommand(GenreSQL, sqlConn, transAction);
            SqlParameter genrePar = new SqlParameter("@genre", SqlDbType.NVarChar, 50);
            genreSqlComm.Parameters.Add(genrePar);
            genreSqlComm.Prepare();

            foreach (Genre genre in genres)
            {
                // Skip any empty or null genre names
                if (string.IsNullOrWhiteSpace(genre.GenreName))
                    continue;

                if (!LoadResult.genreIdMap.ContainsKey(genre.GenreName))
                {
                    // Insert genre and retrieve the generated GenreID
                    genrePar.Value = genre.GenreName;
                    int genreID = (int)genreSqlComm.ExecuteScalar();  // ExecuteScalar to get the GenreID

                    // Store the GenreID in the genreIdMap for future reference
                    LoadResult.genreIdMap[genre.GenreName] = genreID; // Store the GenreID here
                }
            }

            string PersonSQL = "INSERT INTO [Person]([nconst],[primaryname],[birthyear],[deathyear])" +
                "VALUES(@nconst,@primaryname," +
                "@birthyear,@deathyear)";

            SqlCommand PersonSqlComm = new SqlCommand(PersonSQL, sqlConn, transAction);

            SqlParameter nconstPar = new SqlParameter("@nconst",
                SqlDbType.VarChar, 50);
            PersonSqlComm.Parameters.Add(nconstPar);

            SqlParameter primaryNamePar = new SqlParameter("@primaryname",
                SqlDbType.VarChar, 255);
            PersonSqlComm.Parameters.Add(primaryNamePar);

            SqlParameter birthYearPar = new SqlParameter("@birthyear",
                SqlDbType.Int);
            PersonSqlComm.Parameters.Add(birthYearPar);

            SqlParameter deathYearPar = new SqlParameter("@deathyear",
                SqlDbType.Int);
            PersonSqlComm.Parameters.Add(deathYearPar);

            PersonSqlComm.Prepare();
            Console.WriteLine("Person Sql prepared..");

            foreach (Person person in persons)
            {
                nconstPar.Value = person.NConst;
                primaryNamePar.Value = checkObjectForNull(person.PrimaryName);
                birthYearPar.Value = checkObjectForNull(person.BirthYear);
                deathYearPar.Value = checkObjectForNull(person.DeathYear);

                PersonSqlComm.ExecuteNonQuery();
            }
            Console.WriteLine("Person sql command executed..");

            #region Insert Profession
            Console.WriteLine("Starting Profession Insert");
            //Insert til Profession tabelden og hent ProfessionID
            string professionSQL = "INSERT INTO [Profession](Profession) OUTPUT INSERTED.ProfessionID VALUES(@Profession)";
            SqlCommand profSqlComm = new SqlCommand(professionSQL, sqlConn, transAction);
            SqlParameter professionPar = new SqlParameter("@Profession", SqlDbType.NVarChar, 50);
            profSqlComm.Parameters.Add(professionPar);
            profSqlComm.Prepare();

            foreach (string prof in professions)
            {
                if (string.IsNullOrWhiteSpace(prof))
                { continue; }

                // smid professions ind i dictionary hvis den ikke er der
                if (!LoadResult.professionDict.ContainsKey(prof))
                {
                    //insert profession i tabelden
                    professionPar.Value = prof;
                    //hent professionID og set i LoadResult.professionDict
                    int professionID = (int)profSqlComm.ExecuteScalar();
                    LoadResult.professionDict[prof] = professionID;
                }
            }

            #endregion
            /* Mofo giver mig en foreign key conflict, Den skal nok sættes ind i Junction inserteren
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
                tconstKFTPar.Value = kft.tconst;
                nconstKFTPar .Value = kft.nconst;

                KFTSQLComm.ExecuteNonQuery();

                Console.WriteLine("KnownForTitle sql command executed...");
            }
            #endregion
            */

         

            #region Crew insertion
            /*
            string CrewSQL = "INSERT INTO [Crew]([tconst],[directors],[writers])" +
                "VALUES(@tconst,@directors,@writers)";

            SqlCommand CrewSqlComm = new SqlCommand(CrewSQL, sqlConn, transAction);

            SqlParameter tconstCrewPar = new SqlParameter("@tconst",
                               SqlDbType.VarChar, 50);
            CrewSqlComm.Parameters.Add(tconstCrewPar);

            SqlParameter directorsPar = new SqlParameter("@directors",
                               SqlDbType.VarChar, 255);
            CrewSqlComm.Parameters.Add(directorsPar);

            SqlParameter writersPar = new SqlParameter("@writers",
                                              SqlDbType.VarChar, 255);
            CrewSqlComm.Parameters.Add(writersPar);

            CrewSqlComm.Prepare();
            Console.WriteLine("Crew sql command prepared..");

            foreach (Crew crew in crews)
            {
                tconstCrewPar.Value = crew.TConst;
                directorsPar.Value = checkObjectForNull(crew.Directors);
                writersPar.Value = checkObjectForNull(crew.Writers);

                CrewSqlComm.ExecuteNonQuery();

            }
            Console.WriteLine("Crew sql command executed..");
        }
        */
        }
        #endregion
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
