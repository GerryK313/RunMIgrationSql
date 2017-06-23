using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace RunMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            RunMigration();
        }



        public static void RunMigration()
        {
            List<int> MigrationFiles = GetMigrationFiles();
            MigrationFiles.Sort();

            foreach (int fileId in MigrationFiles)
            {
                string searchString = string.Format("{0}*.sql", fileId.ToString());
                string[] MigSqlFile = Directory.GetFiles(GetConfigParam("MigrationPath"), searchString);
                string MigSqlScript = File.ReadAllText(MigSqlFile.FirstOrDefault());
                string result = ExecuteSQL(false, MigSqlScript);
                if(result.Contains("Exception"))
                {
                    Console.WriteLine(string.Format("Error Running Migration Script: {0}, {1}", fileId, result));
                }
                else
                {
                    Console.WriteLine(string.Format("Successfully Ran Migration Script: {0} Which Affected {1} Rows", fileId, result));
                }
            }
            Console.Read();
        }


        public static List<int> GetMigrationFiles()
        {
            // Get Current Migration ID
            int CurrentMigration = GetCurrentMigrationId();

            // Get all MIgration 
            List<int> MigrationFiles = new List<int>();

            DirectoryInfo MigPath = new DirectoryInfo(GetConfigParam("MigrationPath"));
            FileInfo[] Files = MigPath.GetFiles("*.sql");

            foreach (FileInfo Migfile in Files)
            {
                string MigFileName = Migfile.Name;
                MigFileName = MigFileName.Replace('_', '-');
                string[] FileNameArr = MigFileName.Split(new char[] { '-' }, 2);
                int MigFileId = Convert.ToInt32(FileNameArr.FirstOrDefault());

                if (MigFileId > CurrentMigration)
                {
                    MigrationFiles.Add(MigFileId);
                }
            }
            return MigrationFiles;
        }


        public static string GetConfigParam(string param)
        {
            return ConfigurationManager.AppSettings[param];
        }



        public static int GetCurrentMigrationId()
        {
            return Convert.ToInt32(ExecuteSQL(true, GetConfigParam("CurrentMigrationSQL")));
        }



        public static string ExecuteSQL(bool isScalar, string SQL)
        {
            string CleanSQL = SQL.Replace("\r\n", " ").ToString();
            CleanSQL = CleanSQL.Replace(" GO", "").ToString();

            var ConnString = ConfigurationManager.ConnectionStrings["ConnString"].ConnectionString;
            Object returnValue = null;

            using (SqlConnection Conn = new SqlConnection(ConnString))
            {
                using (SqlCommand cmd = new SqlCommand(CleanSQL, Conn))
                {
                    try
                    {
                        Conn.Open();
                    }
                    catch (InvalidOperationException ex) // This will catch SqlConnection Exception
                    {
                        returnValue = "DB Connection Exception issue: " + ex.Message;
                    }

                    if (isScalar)
                    {
                        try
                        {
                            returnValue = cmd.ExecuteScalar();
                        }
                        catch (SqlException ex) // This will catch all SQL exceptions
                        {
                            returnValue = "Execute SQL Exception issue: " + ex.Message;
                        }

                    }
                    else
                    {
                        try
                        {
                                returnValue = cmd.ExecuteNonQuery();
                        }
                        catch (SqlException ex) // This will catch all SQL exceptions
                        {
                            returnValue = "Execute SQL Exception issue: " + ex.Message;
                        }
                    }
                    Conn.Close();
                }
            }
            return returnValue.ToString();
        }
    }
}
