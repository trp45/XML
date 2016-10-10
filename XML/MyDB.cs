using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace XML
{
    class MyDB
    {
        private SqlConnectionStringBuilder connect;
        private string _dbtable;
        private int _batchsize;
        public Dictionary<string, Type> _Mapping;

        public MyDB(Dictionary<string,string> ConnectSettings)
        {
            connect = new SqlConnectionStringBuilder();
            connect.DataSource = ConnectSettings["Server"];
            connect.InitialCatalog = ConnectSettings["DB"];
            connect.UserID = ConnectSettings["User"];
            connect.Password = ConnectSettings["Password"];
        }

        public MyDB(Dictionary<string, string> ConnectSettings, string dbtable, int batchsize)
        {
            connect = new SqlConnectionStringBuilder();
            connect.DataSource = ConnectSettings["Server"];
            connect.InitialCatalog = ConnectSettings["DB"];
            connect.UserID = ConnectSettings["User"];
            connect.Password = ConnectSettings["Password"];

            _dbtable = dbtable;
            _batchsize = batchsize;

            _Mapping = GetDBStruct(dbtable);
        }

        /// <summary>
        /// Поиск и вывод ид БД информации по адресному эелементу
        /// </summary>
        /// <param name="elm"></param>
        /// <param name="parent"></param>
        /// <param name="isAct"></param>
        /// <returns>Возвращает DataTable</returns>
        public DataTable GetElement(string elm, string parent, bool isAct)
        {
            DataTable tab = new DataTable();
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    string sqlcmd = "select case when a1.ACTSTATUS = 1 then 'Актуален' when a1.ACTSTATUS != 1 then 'Не актуален' end ACTSTATUS," +
                        "case when a1.AOLEVEL=1 then 'Регион' when a1.AOLEVEL=2 then 'Автономный округ' when a1.AOLEVEL=3 then 'Район' when a1.AOLEVEL=4 then 'Город' " +
                        "when a1.AOLEVEL=5 then 'Внутригородская территория' when a1.AOLEVEL=6 then 'Населенный пункт' when a1.AOLEVEL=65 then 'Планировочная структура' when a1.AOLEVEL=7 then 'Улица' " +
                        "when a1.AOLEVEL=90 then 'Доп территория' when a1.AOLEVEL=91 then 'Доп территория' end AOLEVEL," +
                        "a1.CODE, a1.FORMALNAME+', '+a1.SHORTNAME FORMALNAME, a1.OFFNAME, a1.SHORTNAME, a2.FORMALNAME+', '+a2.SHORTNAME PARENTNAME , " +
                        "case when a1.CURRSTATUS = 0 then 'Активный' when a1.CURRSTATUS = 51 then 'Переподчиненный' when a1.CURRSTATUS = 99 then 'Несуществующий' else 'Неопределенный' end " +
                    "from TR_ADDROBJ a1 inner join TR_ADDROBJ a2 on a1.PARENTGUID=a2.AOGUID where (a1.FORMALNAME like '%" + elm + "%' or a1.OFFNAME like '%" + elm + "%')" +
                    "and a2.FORMALNAME like '%" + parent + "%'";

                    if (isAct) sqlcmd = sqlcmd + "and a1.ACTSTATUS=1 and a2.ACTSTATUS=1";

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        SqlDataReader dr = cmd.ExecuteReader();
                        tab.Load(dr);
                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }

            return tab;
        }

        /// <summary>
        /// Читает структуру таблиц БД и возвращает словарь "имя поля"-"системный тип"
        /// </summary>
        /// <param name="tab_name">Имя таблицы</param>
        /// <returns>Dictionary<string, Type></returns>
        public Dictionary<string, Type> GetDBStruct(string tab_name)
        {
            Dictionary<string, Type> tab = new Dictionary<string, Type>();


            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    string sqlcmd = "select col.name colname, "+
                        "case when typ.name = 'varchar' then 'System.String' when typ.name = 'int' then 'System.Int32' when typ.name = 'date' then 'System.DateTime' else 'System.String' end coltyp "+
                        "from sys.all_columns col inner join sys.tables tab on col.object_id = tab.object_id inner join sys.types typ on typ.system_type_id = col.system_type_id "+
                        "where tab.name = '" + tab_name + "' order by col.column_id";
                    
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        SqlDataReader dr = cmd.ExecuteReader();
                        while (dr.Read())
                        {
                            tab.Add(dr.GetValue(0).ToString().Trim(), Type.GetType(dr.GetValue(1).ToString().Trim()));
                        }
                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }

            return tab;
        }

        //public void InsertDB(string field, string paramf)
        //{
        //    using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
        //    {
        //        try
        //        {
        //            //insert into <tab> (p1,p2,p3,pn) values (@p1,@p2,@p3,@pn)
        //            //"Insert Into Inventory(CarID, Make, Color, PetName) Values('{0}','{1}','{2}','{3}')", id, make, color, petName);
        //            string sqlcmd = "INSERT INTO ADDROBJ_NEW(" + field + ") VALUES (" + paramf + ")";


        //            conn.Open();
        //            using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
        //            {
        //                SqlParameter param = new SqlParameter();
        //                param.ParameterName = "@CarID";
        //                param.Value = id;
        //                param.SqlDbType = SqlDbType.Int;
        //                cmd.Parameters.Add(param);

        //                cmd.ExecuteNonQuery();

        //                conn.Close();
        //                conn.Dispose();
        //            }
        //        }
        //        catch (SqlException ex)
        //        {
        //            String errorMessage;
        //            errorMessage = "Error: ";
        //            errorMessage = String.Concat(errorMessage, ex.Message);
        //            throw new Exception(errorMessage);
        //        }
        //        finally
        //        {
        //            if (conn != null)
        //            {
        //                conn.Close();
        //                conn.Dispose();
        //            }
        //        }
        //    }
        //}

        public void BulkInsertAll(DataTable toinsert)
        {
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    conn.Open();
                    using (SqlBulkCopy copy = new SqlBulkCopy(conn))
                    {
                        //foreach (DataColumn s in toinsert.Columns)
                        //{
                        //    copy.ColumnMappings.Add(s.ToString(), s.ToString());
                        //}

                        copy.DestinationTableName = _dbtable;
                        copy.BatchSize = _batchsize;
                        //copy.SqlRowsCopied += new SqlRowsCopiedEventHandler(copy_SqlRowsCopied);

                        copy.WriteToServer(toinsert);
                    }
                }
                catch (Exception ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
        }

        public int ReadVersion()
        {
            int Ret = -1;
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    string sqlcmd = "SELECT ISNULL(MAX(versionId),0) VerID FROM "+conn.Database+".[dbo].[data_version]";
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        SqlDataReader dr = cmd.ExecuteReader();
                        while (dr.Read())
                        {
                            Ret = (int)dr.GetValue(0);
                        }
                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
            return Ret;
        }

        public void WriteVersion(DateTime datetime, int ifnsver)
        {
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    conn.Open();
                    SqlCommand command = new SqlCommand("INSERT INTO data_version (date_update, versionId) VALUES (@date_update, @versionId)", conn);

                    command.Parameters.Add("@date_update", SqlDbType.DateTime, 1, "date_update");
                    command.Parameters["@date_update"].Value = datetime;

                    command.Parameters.Add("@versionId", SqlDbType.Int , 1, "ifnsver");
                    command.Parameters["@versionId"].Value = ifnsver;

                    command.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
        }

        public void Merge(string tab)
        {
            //TODO: Пока merge работает только для таблицы ADDROBJ, надо как-то определять ключевые поля. Скорее всего делать перечисление для каждой таблицы
            string dstTab = tab.Substring(0,tab.Length-4);
            //_Mapping = GetDBStruct(dstTab);

            StringBuilder update = new StringBuilder("");
            StringBuilder insert = new StringBuilder("");
            
            foreach (KeyValuePair<string, Type> str in _Mapping)
            {
                update.Append(str.Key.ToString()); update.Append(" = src."); update.Append(str.Key.ToString()); update.Append(",");
                insert.Append(str.Key.ToString()); insert.Append(",");
            }
            update.Remove(update.Length - 1, 1);
            insert.Remove(insert.Length - 1, 1);

            List<string> KeyField = GetKeyField(dstTab);
            StringBuilder kField = new StringBuilder("");
            foreach (string str in KeyField)
            {
                kField.Append("dst."); kField.Append(str); kField.Append(" = src."); kField.Append(str); kField.Append(" and ");
            }
            kField.Remove(kField.Length - 5, 5);

            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    //string sqlcmd = "MERGE " + dstTab + " AS dst USING " + tab + " AS src ON dst.AOGUID = src.AOGUID and dst.AOID = src.AOID " +
                    string sqlcmd = "MERGE " + dstTab + " AS dst USING " + tab + " AS src ON "+ kField +
                        " WHEN MATCHED THEN UPDATE SET " + update + " WHEN NOT MATCHED THEN INSERT(" + insert + ")" +
                        "VALUES(" + insert + ");";

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        //TODO: Для больших объемов тут будет жопа из-за таймаутов, надо думать
                        cmd.CommandTimeout = 0;
                        cmd.ExecuteNonQuery();
                        
                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
        }

        public void Truncate(string tab)
        {
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {

                    string sqlcmd = "truncate table " + tab;

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        cmd.ExecuteNonQuery();

                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);

                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
        }

        public void DeleteAnotherRegion(int code)
        {
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    conn.Open();
                    SqlCommand command = new SqlCommand("delete from ADDROBJ where REGIONCODE!=@regcode", conn);
                    command.Parameters.AddWithValue("@regcode", code);
                    command.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
            }
        }

        //TODO: Работает только для ADDROBJ, и не дописал
        /// <summary>
        /// Необходима для переброса данных в трансформированные таблицы, для более простой связи с 1С
        /// </summary>
        public void ConvertTable()
        {
            //_Mapping
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    string sqlcmd = "INSERT INTO TR_ADDROBJ "+
                        " SELECT ACTSTATUS, CONVERT(uniqueidentifier, AOGUID), CONVERT(uniqueidentifier, AOID), AOLEVEL, AREACODE, AUTOCODE, CENTSTATUS, CITYCODE, CAST(CODE + REPLICATE('0', 25 - LEN(CODE)) as numeric(26, 0)) as CODE, CURRSTATUS, ENDDATE, FORMALNAME, CONVERT(uniqueidentifier, NEXTID), "+
	                    " OFFNAME, OKATO, OKTMO, OPERSTATUS, CONVERT(uniqueidentifier, PARENTGUID), PLACECODE, PLAINCODE, POSTALCODE, CONVERT(uniqueidentifier, PREVID), REGIONCODE, SHORTNAME, STARTDATE, STREETCODE, UPDATEDATE, CTARCODE, EXTRCODE, SEXTCODE, LIVESTATUS, CONVERT(uniqueidentifier, NORMDOC) "+
                        " FROM ADDROBJ; ";

                    conn.Open();
                    SqlCommand command = new SqlCommand(sqlcmd, conn);
                    command.CommandTimeout = 120;
                    command.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
            }
        }

        /// <summary>
        /// Поиск и вывод из БД информации по дому
        /// </summary>
        /// <param name="hnum">Поиск по номеру дома</param>
        /// <param name="elemet">Может быть поиск по адресному элементу</param>
        /// <param name="parent">Может быть поиск по родителю</param>
        /// <param name="isAct">Поиск с учетом актуальность ФИАС</param>
        /// <returns>Возвращает DataTable</returns>
        public DataTable GetHouse(string hnum, string elemet, string parent, bool isAct)
        {
            DataTable tab = new DataTable();
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    string sqlcmd = "select case when a1.ACTSTATUS = 1 then 'Актуален' when a1.ACTSTATUS != 1 then 'Не актуален' end ACTSTATUS," +
                        "case when a1.CURRSTATUS = 0 then 'Активный' when a1.CURRSTATUS = 51 then 'Переподчиненный' when a1.CURRSTATUS = 99 then 'Несуществующий' else 'Неопределенный' end, " +
                        "a2.FORMALNAME+', '+a2.SHORTNAME PARENTNAME, a1.FORMALNAME+', '+a1.SHORTNAME FORMALNAME, a1.OFFNAME, a1.SHORTNAME, h.HOUSENUM, h.BUILDNUM, h.STRUCNUM," +
                        "case when a1.AOLEVEL=1 then 'Регион' when a1.AOLEVEL=2 then 'Автономный округ' when a1.AOLEVEL=3 then 'Район' when a1.AOLEVEL=4 then 'Город' " +
                        "when a1.AOLEVEL=5 then 'Внутригородская территория' when a1.AOLEVEL=6 then 'Населенный пункт' when a1.AOLEVEL=65 then 'Планировочная структура' when a1.AOLEVEL=7 then 'Улица' " +
                        "when a1.AOLEVEL=90 then 'Доп территория' when a1.AOLEVEL=91 then 'Доп территория' end AOLEVEL, " +
                        "st.NAME KINDBUILD, es.NAME TYPEDOM " +
                    "from TR_ADDROBJ a1 inner join TR_ADDROBJ a2 on a1.PARENTGUID=a2.AOGUID " +
                    "left join TR_HOUSE h on h.AOGUID=a1.AOGUID left join ESTSTAT es on es.ESTSTATID=h.ESTSTATUS left join STRSTAT st on st.STRSTATID=h.STRSTATUS " +
                    "where (a1.FORMALNAME like '%" + elemet + "%' or a1.OFFNAME like '%" + elemet + "%')" +
                    "and a2.FORMALNAME like '%" + parent + "%'" +
                    "and(h.HOUSENUM like '%" + hnum + "%' or h.HOUSENUM is null)";

                    if (isAct) sqlcmd = sqlcmd + "and a1.ACTSTATUS=1 and a2.ACTSTATUS=1";

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        SqlDataReader dr = cmd.ExecuteReader();
                        tab.Load(dr);
                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }

            return tab;
        }

        /// <summary>
        /// Нужна для нахождения полей таблиц по которым пойдет merge
        /// </summary>
        /// <param name="tabname">Имя таблицы, по полям которой "пойдет" merge</param>
        /// <returns>Список </returns>
        public List<string> GetKeyField(string tabname)
        {
            List<string> Ret = new List<string>();
            switch (tabname)
            {
                case "ADDROBJ":
                    Ret.Add("AOGUID"); Ret.Add("AOID");
                    break;
                case "HOUSE":
                    Ret.Add("HOUSEGUID"); Ret.Add("HOUSEID");
                    break;
            }

            return Ret;
        }

        //TODO: DROP указанной таблицы, недописал
        public void DropTable(string tab)
        {
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {

                    string sqlcmd = "CREATE TABLE [dbo].[" + tab + "](";

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        cmd.ExecuteNonQuery();

                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);

                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
        }

        //TODO: Недописал, по идее надо вытаскивать из XSD структуру БД и созавать текст запроса
        public void CreateTable(string tab)
        {
            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {

                    string sqlcmd = "CREATE TABLE [dbo].[" + tab + "](";

                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(sqlcmd, conn))
                    {
                        cmd.ExecuteNonQuery();

                        conn.Close();
                        conn.Dispose();
                    }
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);

                    throw new Exception(errorMessage);
                }
                finally
                {
                    if (conn != null)
                    {
                        conn.Close();
                        conn.Dispose();
                    }
                }
            }
        }
    }
}
