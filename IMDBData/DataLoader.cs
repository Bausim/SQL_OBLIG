using IMDBData.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace IMDBData
{
    public class DataLoader
    {
        // gør brug af hashset til professions tabelden/modellen
        //HashSet<string> professions;
        int lineCount = 0;
        LoadResult result = new LoadResult();
        public LoadResult LoadTitles(string filePath)
        {
            
            foreach (string line in File.ReadLines(filePath).Skip(1))
            {
                if (lineCount == 50000)
                {
                    break;
                }
                string[] splitLine = line.Split("\t");
                if (splitLine.Length != 9)
                {
                    throw new Exception("Invalid line:" + line);
                }

                string tconst = splitLine[0];
                //smider tconst ind i et HashSet til KnownForTitles insertion check senere
                LoadResult.tconstHS.Add(tconst);

                string primaryTitle = splitLine[2];
                string originalTitle = splitLine[3];
                bool isAdult = splitLine[4] == "1";
                int? startYear = ParseInt(splitLine[5]);
                int? endYear = ParseInt(splitLine[6]);
                int? runtimeMinutes = ParseInt(splitLine[7]);


                result.Titles.Add(new()
                {
                    TConst = tconst,
                    PrimaryTitle = primaryTitle,
                    OriginalTitle = originalTitle,
                    IsAdult = isAdult,
                    StartYear = startYear,
                    EndYear = endYear,
                    RuntimeMinutes = runtimeMinutes
                });

                // Handle genres from the 9th column (splitLine[8])
                string[] genreArray = splitLine[8].Split(',');

                foreach (string genreName in genreArray)
                {
                    // Skip invalid genres
                    if (string.IsNullOrWhiteSpace(genreName) || genreName.Trim() == @"\N")
                    {
                        continue;
                    }


                    string trimmedGenreName = genreName.Trim();

                    //Smid genres ind i HashSettet
                    //Genres.Add(trimmedGenreName);


                    Genre existingGenre = result.Genres.FirstOrDefault(g => g.GenreName == trimmedGenreName);


                    if (existingGenre == null)
                    {
                        existingGenre = new Genre { GenreName = trimmedGenreName };
                        result.Genres.Add(existingGenre);
                    }

                    
                }
                lineCount++;
            }
            return result;
        }
        public LoadResult LoadPersons(string filePath)
        {
            lineCount = 0;
            //List<Person> persons = new List<Person>();
            foreach (string line in File.ReadLines(filePath).Skip(1))
            {


                if (lineCount == 50000)
                {
                    break;
                }
                string[] splitLine = line.Split("\t");
                if (splitLine.Length != 6)
                {
                    throw new Exception("Invalid line:" + line);
                }

                string nconst = splitLine[0];
                LoadResult.nconstHS.Add(nconst);
                string primaryName = splitLine[1];
                int? birthYear = ParseInt(splitLine[2]);
                int? deathYear = ParseInt(splitLine[3]);

                result.persons.Add(new()
                {
                    NConst = nconst,
                    PrimaryName = primaryName,
                    BirthYear = birthYear,
                    DeathYear = deathYear
                });
                //Load PrimaryProfessions
                string[] professionArray = splitLine[4].Split(",");
                //kør igennem prof array
                foreach (string profession in professionArray)
                {
                    if (string.IsNullOrEmpty(profession) || profession.Trim() == @"\N")
                    { continue; }

                    string trimmedProfession = profession.Trim();
                    result.professions.Add(trimmedProfession);
                }

                // Load KnownForTitles
                string[] knownForTitles = splitLine[5].Split(",");

                foreach (string knownForTitle in knownForTitles)
                {
                    if (string.IsNullOrEmpty(knownForTitle) || knownForTitle.Trim() == @"\N")
                    { continue; }

                    string trimmedKFT = knownForTitle.Trim();
                    result.knownForTitles.Add(new()
                    {
                        nconst = nconst,
                        tconst = knownForTitle,
                    });

                    
                }
                lineCount++;
            }

                return result;
            
        }
        public LoadResult LoadCrews(string filePath)
        {
            lineCount = 0;
            List<Crew> crews = new List<Crew>();
            foreach (string line in File.ReadLines(filePath).Skip(1))
            {
                if (lineCount == 50000)
                {
                    break;
                }
                string[] splitLine = line.Split("\t");
                if (splitLine.Length != 3)
                {
                    throw new Exception("Invalid line:" + line);
                }
                /*
                 *  string[] professionArray = splitLine[4].Split(",");
                //kør igennem prof array
                foreach (string profession in professionArray)
                {
                    if (string.IsNullOrEmpty(profession) || profession.Trim() == @"\N")
                    { continue; }

                    string trimmedProfession = profession.Trim();
                    result.professions.Add(trimmedProfession);
                }

                 */

                string tconst = splitLine[0];
                string[] directorsArray = splitLine[1].Split(",", StringSplitOptions.RemoveEmptyEntries);
                string[] writersArray = splitLine[2].Split(",", StringSplitOptions.RemoveEmptyEntries);

                foreach (string director in directorsArray)
                {

                    //Check om de loadede Titles tconst er i vores HashSet
                    if (LoadResult.tconstHS.Contains(tconst))
                    {
                        if (!string.IsNullOrEmpty(director) && director.Trim() != @"\N")
                        {
                            result.titleDirector.Add(new TitleDirector
                            {
                                TConst = tconst,
                                NConst = director.Trim()
                            });
                        }

                        foreach (string writer in writersArray)
                        {
                            if (!string.IsNullOrEmpty(writer) && writer.Trim() != @"\N")
                            {
                                result.titleWriters.Add(new TitleWriter
                                {

                                    TConst = tconst,
                                    NConst = writer.Trim()

                                });
                            }

                        }
                    }
                
                }

                

             

                lineCount++;
            }
            return result;
        }

        int? ParseInt(string value)
        {
            if (value.ToLower() == "\\n") // checks if it is \n
                {
                    return null;
                }
                return int.Parse(value);

        }
    }
}
