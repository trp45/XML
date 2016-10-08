using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using System.Data.OleDb;

namespace XML
{
    class MyExcel
    {
        private string _xlsPath;
        private FileInfo _file;

        public MyExcel(string xlsPath)
        {
            _xlsPath = xlsPath;
            _file = new FileInfo(_xlsPath);
        }

        protected string GetExcelConnectionString()
        {
            Dictionary<string, string> props = new Dictionary<string, string>();

            if (_file.Extension == ".xlsx")
            {                                                                   
                props["Provider"] = "Microsoft.ACE.OLEDB.12.0;";
                props["Extended Properties"] = "Excel 12.0 XML";
                props["Data Source"] = _file.FullName;
            }
            else if (_file.Extension == ".xls")
            {
                props["Provider"] = "Microsoft.Jet.OLEDB.4.0";
                props["Extended Properties"] = "Excel 8.0";
                props["Data Source"] = _file.FullName;
            }
            else throw new Exception("Неизвестное расширение файла!");

            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<string, string> prop in props)
            {
                sb.Append(prop.Key);
                sb.Append('=');
                sb.Append(prop.Value);
                sb.Append(';');
            }
            return sb.ToString();
        }


        public void loadExcelFile(int fType)
        {
            // пока напишу только для формата МинФина, и вообще не дописал
            DataSet ds = new DataSet();
            string connectionString = GetExcelConnectionString();

            using (OleDbConnection conn = new OleDbConnection(connectionString))
            {

                conn.Open();
                OleDbCommand cmd = new OleDbCommand();
                cmd.Connection = conn;

                DataTable dtSheet = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);

                try
                { 
                    // Loop through all Sheets to get data
                    foreach (DataRow dr in dtSheet.Rows)
                    {
                        string sheetName = dr["TABLE_NAME"].ToString();
                        if (!sheetName.Contains("FilterDatabase"))
                        {
                            // Get all rows from the Sheet                              
                            cmd.CommandText = "SELECT * FROM [" + sheetName + "]";

                            DataTable dt = new DataTable();
                            dt.TableName = sheetName;

                            OleDbDataAdapter da = new OleDbDataAdapter(cmd);
                            da.Fill(dt);

                            ds.Tables.Add(dt);
                        }
                    }

                    //тут читаем и куда-то выводим
                    for (int j = 0; j < ds.Tables[0].Columns.Count; j++)
                        for (int i = 4; i < ds.Tables[0].Rows.Count; i++)
                        {
                            //var value = ds.Tables[0].Rows[i].ItemArray[j];
                        }
                }
                catch (Exception e)
                {
                    ds.Dispose();
                    throw new Exception(e.Message);
                }
            }
        }
    }
}
