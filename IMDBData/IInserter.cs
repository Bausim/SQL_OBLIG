using System;
using IMDBData.Models;
using System.Data.SqlClient;

namespace IMDBData
{
    public interface IInserter
    {
       
        void Insert(List<Title> titles, List<Person> persons, List<Genre> genres, HashSet<string> professions, SqlConnection sqlConn, SqlTransaction transAction);
    }
}
