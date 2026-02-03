using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.Data.Odbc;
using System.Data;
using System.Collections;

using System.Data.OleDb;
using System.Data.SqlClient;
using System.Configuration;

namespace BIDVAutoVS2022
{
    internal class common_sql
    {
        static string USER_DB = "openpg";
        static string PASS_DB = "openpgpwd";
        static public string DATABASE_SQL_FTP_SIBOR = "server=10.130.2.14;connect timeout=3600; uid=sa;pwd=sa;database=dbftosql";
        static public string DATABASE_SQL_FTP_CURRENT = "server=10.130.2.6;connect timeout=3600; uid=sa;pwd=sa;database=bsms2";
        static public string DATABASE_SQL_QL_FTP = "server=10.130.2.8;connect timeout=3600; uid=sa;pwd=sa;database=QL_FTP";
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


        static private NpgsqlConnection OpenConnections(string server, int port, string database_name)
        {
            //string connstring = String.Format("Server={0};Port={1};Database={2};User Id={3};Password={4};Encoding=UNICODE;CommandTimeout=20;",
            //    server, port, database_name, USER_DB, PASS_DB);
            string connstring = String.Format("Server={0};Port={1};Database={2};User Id={3};Password={4};CommandTimeout=20;",
                server, port, database_name, USER_DB, PASS_DB);
            NpgsqlConnection connAccount;
            connAccount = new NpgsqlConnection(connstring);
            connAccount.Open();
            return connAccount;
        }

        static public DataTable GetData(string sql_execute, string server, int port, string database_name)
        {
            NpgsqlConnection connAccount = OpenConnections(server, port, database_name);
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql_execute, connAccount);
            DataSet ds = new DataSet();
            da.Fill(ds);
            DataTable dt = new DataTable();
            dt = ds.Tables[0];
            connAccount.Close();
            connAccount = null;
            return dt;
        }

        static public int ExecuteNoneQueryPostgree(string sql_execution, string server, int port, string database_name)
        {
            int iResult = 0;
            //NpgsqlConnection connAccount = OpenConnections(server, port, database_name);
            string connstring = String.Format("Server={0};Port={1};Database={2};User Id={3};Password={4};CommandTimeout=20;",
                server, port, database_name, USER_DB, PASS_DB);
            NpgsqlConnection connAccount;
            connAccount = new NpgsqlConnection(connstring);
            connAccount.Open();
            NpgsqlTransaction transaction = connAccount.BeginTransaction();
            using (NpgsqlCommand pgsqlcommand = new NpgsqlCommand(sql_execution, connAccount))
            {
                pgsqlcommand.CommandTimeout = 3600;
                iResult = pgsqlcommand.ExecuteNonQuery();
            }
            transaction.Commit();
            connAccount.Close();
            connAccount = null;
            return iResult;
        }

        static public int ExecuteExecuteScalarPostgree(string sql_execution, string server, int port, string database_name)
        {
            int iResult = 0;
            NpgsqlConnection connAccount = OpenConnections(server, port, database_name);
            sql_execution = sql_execution.Trim(';') + " returning id;";
            using (NpgsqlCommand pgsqlcommand = new NpgsqlCommand(sql_execution, connAccount))
            {
                pgsqlcommand.CommandTimeout = 3600;
                iResult = Convert.ToInt32(pgsqlcommand.ExecuteScalar());
            }
            connAccount.Close();
            connAccount = null;
            return iResult;
        }

        public static DataTable GetDataFoxPro(string folder_path, string sQuery, bool bReader)
        {
            DataTable dt = new DataTable();
            string sConn = "DSN=SYS_FOX_DSN;SourceDB=" + folder_path + ";Deleted=Yes;Collate=Machine;Exclusive=No;BackgroundFetch=Yes;SourceType=DBF;Null=Yes;UID=";
            //string sConn = "Driver={Microsoft Visual FoxPro Driver};SourceDB=" + @"F:\DLGGetAuto\2019DLG\" + ";Deleted=Yes;Collate=Machine;Exclusive=No;BackgroundFetch=Yes;SourceType=DBF;Null=Yes;UID=";
            log.Error(string.Format("sConn : {0}", sConn));
            //string sConn = "Provider=vfpoledb;DSN=ODBC_FOX";
            try
            {
                using (OdbcConnection oConn = new OdbcConnection(sConn))
                //using (OleDbConnection oConn = new OleDbConnection(sConn))
                {
                    oConn.Open();
                    //OleDbCommand command = new OleDbCommand(sQuery, oConn);
                    //using (OleDbDataAdapter oda = new OleDbDataAdapter(command))
                    using (OdbcDataAdapter oda = new OdbcDataAdapter(sQuery, oConn))
                    {
                        oda.Fill(dt);
                    }
                    oConn.Close();
                }
            }
            catch (Exception ex)
            {
                log.Error(string.Format("loi doc file fox dbf {0}", ex.Message));
                Console.WriteLine(ex.Message);
            }
            return dt;
        }

        public static OleDbConnection ConnectionFoxPro(string sConn)
        {
            OleDbConnection oConn = null;
            try
            {
                using (oConn = new OleDbConnection(sConn))
                {
                    oConn.Open();
                }
            }
            catch (Exception ex)
            {
                return null;
            }
            return oConn;
        }

        static public DataTable MSSQL_GetDataTable(string sql_execution, string conn_string)
        {
            DataTable dt = new DataTable();
            SqlConnection sqlConn = new SqlConnection();
            SqlConnection.ClearAllPools();
            sqlConn.ConnectionString = conn_string;
            sqlConn.Open();
            try
            {
                SqlCommand cmd = new SqlCommand(sql_execution, sqlConn);
                cmd.CommandTimeout = 3600;
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(dt);
                sqlConn.Close();
            }
            catch
            {
                sqlConn.Close();
            }
            return dt;
        }

        static public DataTable MSSQL_GetDataTable_Diff(string sql_execution, string conn_string)
        {
            DataTable dt = new DataTable();
            using (OdbcConnection oConn = new OdbcConnection(conn_string))
            {
                oConn.Open();
                using (OdbcCommand oCmd = oConn.CreateCommand())
                {
                    oCmd.CommandText = sql_execution;
                    dt.Load(oCmd.ExecuteReader());
                }
                oConn.Close();
            }
            return dt;
        }

        static public int MSSQL_ExecuteNoneQuery(string sql_execution, string conn_string)
        {
            int iResult = 0;
            SqlConnection sqlConn = new SqlConnection(conn_string);
            sqlConn.Open();
            SqlCommand cmd = new SqlCommand(sql_execution, sqlConn);
            cmd.CommandTimeout = 3600;
            iResult = Convert.ToInt32(cmd.ExecuteScalar());
            sqlConn.Close();
            sqlConn.Dispose();
            if (iResult == 0) // truong hop thanh cong ma ket qua tra ve 0 nen can phai set gia tri la 1
            {
                iResult = 1;
            }
            try
            {
            }
            catch
            {
                sqlConn.Close();
                sqlConn.Dispose();
            }
            return iResult;
        }
    }
}
