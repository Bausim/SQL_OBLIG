// See https:aka.ms/new-console-template for more information
// Console.WriteLine("Hello, World!");

using IMDBData;
using IMDBData.Models;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;

// --- INSERTER CLASSES ---
// Handles bulk insertion of data into the database
IInserter inserter = null;
//gør det lige simpelt fordi jeg vil se om det virker
//senere hen prøv at se om den kan arve fra IInserter på en smartere måde
JunctionPreparedInserter JPI = new JunctionPreparedInserter();
DataLoader dataLoader = new DataLoader();
JunctionDataLoader JDL = new JunctionDataLoader();
bool shouldInsert = false;
SqlConnection sqlConn = new SqlConnection(@"Server=(LocalDB)\MSSQLLocalDB;Integrated Security=true;AttachDbFileName=C:\Users\simon\IMDB_OB.mdf");
sqlConn.Open();

// --- READER CLASS ---
// Handles all functionality for searching the database
Reader reader = new Reader();
SearchService searchService = new SearchService(sqlConn);

// --- USER INPUT ---
Console.WriteLine(
    "What do you want to do? \n" +
    " 1. Use normal inserter \n" +
    " 2. Use prepared inserter \n" +
    " 3. search for a movie by title \n" +
    " 4. search for a person by name \n" +
    " 5. add a movie to the database \n" +
    " 6. Add a person to the database \n" +
    " 7. Update Movie information. \n" +
    " 8. Delete Movie information. \n" +
    " ---------------------------------");


string? input = Console.ReadLine();


// --- SWITCH STATEMENT ---
switch (input)
{
    case "1":
        shouldInsert = true;
      //  inserter = new NormalInserter(); // KEEP OR REMOVE \[T]/
        break;

    case "2": 
        shouldInsert = true;
        inserter = new PreparedInserter();
        break;

    case "3":
        Console.WriteLine("Enter the title of the movie you want to search for");
        string title = Console.ReadLine();
        Console.WriteLine("You've searched for movies including: \n" + title + "\nSearching...");
        //reader.SearchForMovieByTitle(title);
        searchService.SearchTitle(title);

        break;
    
    case "4":
        Console.WriteLine("Enter the name of the person you want to search for: ");
        string name = Console.ReadLine();
        Console.WriteLine("You've searched for people including: \n" + name + "\nSearching...");
        searchService.SearchPerson(name);
        break;

    case "5":
        string isBool;
        Console.WriteLine("Enter the titleType of the movie you want to add");
        string titleType = Console.ReadLine();
        Console.WriteLine("Enter the primary title of the movie you want to add:");
        string primaryTitle = Console.ReadLine();
        Console.WriteLine("Enter the original title of the movie you want to add:");
        string originalTitle = Console.ReadLine();
        Console.WriteLine("Is Adult? (false, true)");
        bool isAdult = bool.Parse(Console.ReadLine());
        Console.WriteLine("Enter the year of release, of the movie you want to add:");
        int startYear = int.Parse(Console.ReadLine());
        Console.WriteLine("Enter the end year of the movie you want to add:");
        int endYear = int.Parse(Console.ReadLine());
        Console.WriteLine("Enter the runtime of the movie you want to add (in minutes):");
        int runtimeMinutes = int.Parse(Console.ReadLine());
        AddMovieOrPerson.AddMovie(titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, sqlConn);
        
        break;

    case "6":
        Console.WriteLine("Enter the name of the person you want to add: ");
        string ActorName = Console.ReadLine();
        Console.WriteLine("Enter the birth year of the person you want to add: ");
        int birthYear = int.Parse(Console.ReadLine());
        Console.WriteLine("Enter the deathyear of the person");
        int deathYear = int.Parse(Console.ReadLine());
        AddMovieOrPerson.AddPerson(ActorName, birthYear, deathYear, sqlConn);
        break;

    case "7":
        Console.WriteLine("Enter the Id of the movie you want to update: ");
        string movieIdU = Console.ReadLine();
        //cw write out the movie information from the id provided.
        
        AddMovieOrPerson.UpdateMovie(movieIdU, sqlConn);
        break;

    case "8":
        Console.WriteLine("Enter the Id of the movie you want to Delete: ");
        string movieId = Console.ReadLine();
        AddMovieOrPerson.DeleteMovie(movieId, sqlConn);
        break;

    default:
        throw new Exception("Invalid input");
}

if (shouldInsert)
{
    // load data from files
    string titleFilePath = "C:/temp/tempData/title.basics.tsv";
    string personFilePath = "C:/temp/tempData/name.basics.tsv";
    string crewFilePath = "C:/temp/tempData/title.crew.tsv";

    // take data from files and put them into lists
    LoadResult titleData = dataLoader.LoadTitles(titleFilePath);
    LoadResult personData = dataLoader.LoadPersons(personFilePath);
    LoadResult crewsData = dataLoader.LoadCrews(crewFilePath);

    // Print the length of the lists
    Console.WriteLine("List of titles length: " + titleData.Titles.Count);
    Console.WriteLine("List of persons length: " + personData.persons.Count);
    Console.WriteLine("List of Directors length: " + crewsData.titleDirector.Count);
    Console.WriteLine("List of Writers length: " + crewsData.titleWriters.Count);

    // --- TRY CONNECTION TO DATABASE ---
    //SqlConnection sqlConn = new SqlConnection(@"Server=(LocalDB)\MSSQLLocalDB;Integrated Security=true;AttachDbFileName=C:\Users\simon\IMDB_OB.mdf");
   /*
    try
    {
         sqlConn.Open();
    }
    catch (SqlException ex)
    {
        Console.WriteLine("SQL Exception: " + ex.Message);
    }
    catch (Exception ex)
    {
        Console.WriteLine("General Exception: " + ex.Message);
    }
   */
    SqlTransaction transAction = sqlConn.BeginTransaction();

    DateTime before = DateTime.Now;

    // --- TRY TO INSERT DATA INTO DATABASE ---
    try
    {
        inserter.Insert(titleData.Titles, personData.persons, titleData.Genres, personData.professions, sqlConn, transAction);
        transAction.Commit();
        Console.WriteLine("Insertion finished. Now onto the junction insertion");
        
        
        // transAction.Rollback();

    }
    catch (Exception e)
    {
        Console.WriteLine("inserter failed: " + e.Message);
        transAction.Rollback();
    }

    // Ny SqlTransaction til Junction Tabelderne 
    SqlTransaction junctionTransAction = sqlConn.BeginTransaction();

    try 
    {
        Console.WriteLine("Load complete, now inserting");
        LoadResult titleGenre = JDL.LoadTitleGenres(titleFilePath);
        LoadResult ppData = JDL.LoadPP(personFilePath);
        //Nok bedre at ændre den her til Dictionary i stedet for at loade hele Name filen igen
        LoadResult personKFTData = dataLoader.LoadPersons(personFilePath);

        JPI.Insert(titleGenre.TitleGenres, ppData.personProfessionn, personKFTData.knownForTitles, crewsData.titleDirector, crewsData.titleWriters,  sqlConn, junctionTransAction);
        Console.WriteLine("Trying to Commit");
        junctionTransAction.Commit();
    }
    catch(Exception e)
    {
        Console.WriteLine("inserter failed: " + e.Message);
        junctionTransAction.Rollback();
    }
    DateTime after = DateTime.Now;

    // --- CLOSE CONNECTION ---
    sqlConn.Close();

    // --- PRINT TIME TAKEN FOR INSERTION---
    Console.WriteLine("Milliseconds passed: " + (after - before).TotalMilliseconds);

  
}

