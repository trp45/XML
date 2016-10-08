using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Xml;
using System.Xml.Schema;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Collections;

namespace XML
{
    //Глобальный TODO
    //SOAP запросы на сервера налоговой
    public partial class main_form : Form
    {
        string ExcelPath;
        BackgroundWorker bw;

        Dictionary<string, string> workfiles = null;

        public main_form()
        {
            InitializeComponent();
            //pbUpload.Maximum = 100;
            //pbUpload.Step = 1;

            bw = new BackgroundWorker();
            bw.WorkerReportsProgress = true;
            bw.WorkerSupportsCancellation = true;

            bw.DoWork += new DoWorkEventHandler(bw_DoWork);
            bw.ProgressChanged += new ProgressChangedEventHandler(bw_ProgressChanged);
            bw.RunWorkerCompleted += new RunWorkerCompletedEventHandler(bw_RunWorkerCompleted);

        }

        public class LoadXMLInput
        {
            public Dictionary<string, string> ConnectSettings
            { get; set; }

            public string tbname
            { get; set; }

            public string xmlpath
            { get; set; }

            public string xsdpath
            { get; set; }

            public DateTime dt
            { get; set; }

            public int dbVer
            { get; set; }

            public bool dbFreeDate
            { get; set; }

            public LoadXMLInput(Dictionary<string, string> _ConnectSettings, string _tbname, string _xmlpath, string _xsdpath, DateTime _dt, int _dbVer, bool _dbFreeDate)
            {
                ConnectSettings = _ConnectSettings;
                xmlpath = _xmlpath;
                xsdpath = _xsdpath;
                tbname = _tbname;
                dt = _dt;
                dbVer = _dbVer;
                dbFreeDate = _dbFreeDate;
            }
        }

        private void bw_DoWork(object sender, DoWorkEventArgs e)
        {
            //BackgroundWorker worker = (BackgroundWorker)sender;
            LoadXMLInput input = (LoadXMLInput)e.Argument;

            try
            {
                MyXmlReader xml = new MyXmlReader(input.tbname, input.xmlpath, input.xsdpath, input.ConnectSettings, 500);
                xml.AsyncRead(bw, e, input.dt, input.dbVer, input.dbFreeDate);
            }
            catch (Exception ex)
            {
                MessageBox.Show("Произошла ошибка, подробнее в логе");
                MessageBox.Show(ex.Message);
                //TODO: Ошибки из другого потока
                //Logs.AppendText(ex.Message);
                //Logs.AppendText("\r\n");
                //Logs.AppendText(ex.StackTrace);
                //Logs.AppendText("\r\n");
            }

            if (bw.CancellationPending)
            {
                e.Cancel = true;
                return;
            }

            // Вернуть результат
            //e.Result = primes;
        }

        private void bw_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            pbUpload.Value = e.ProgressPercentage;
        }

        private void bw_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled)
            {
                MessageBox.Show("Загрузка отменена");
            }
            else if (e.Error != null)
            {
                // Ошибка была сгенерирована обработчиком события DoWork
                MessageBox.Show(e.Error.Message, "Произошла ошибка, подробнее в логе");
                Logs.AppendText(e.Error.Message);
                Logs.AppendText("\r\n");
                Logs.AppendText(e.Error.StackTrace);
                Logs.AppendText("\r\n");
            }
            else
            {
                //int[] primes = (int[])e.Result;
                //foreach (int prime in primes) lstPrimes.Items.Add(prime);
                MessageBox.Show("Все загружено");
                Text = "ФИАС";
                pbUpload.Value = 0;
            }
            btnManualLoadXml.Enabled = true;
            //cancelButton.Enabled = false;
        }

        // TODO: Создание файла со структурой БД из XSD
        private void btn_OpenXSD_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog opn = new FolderBrowserDialog();
            opn.ShowNewFolderButton = false;

            if (opn.ShowDialog() == DialogResult.OK)
            {
                try
                {
                    tbox_SchemePath.Text = opn.SelectedPath;
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Ошибка: Невозможно прочитать файл с диска. Текст ошибки: " + ex.Message);
                }
            }

        }

        private void btn_TestConnect_Click(object sender, EventArgs e)
        {
            SqlConnectionStringBuilder connect = new SqlConnectionStringBuilder();

            connect.DataSource = tbox_Srv.Text;
            connect.InitialCatalog = tbox_DB.Text;
            connect.UserID = tbox_Login.Text;
            connect.Password = tbox_Pass.Text;

            using (SqlConnection conn = new SqlConnection(connect.ConnectionString))
            {
                try
                {
                    conn.Open();
                    MessageBox.Show("Успех");
                    conn.Close();
                    conn.Dispose();
                }
                catch (SqlException ex)
                {
                    String errorMessage;
                    errorMessage = "Error: ";
                    errorMessage = String.Concat(errorMessage, ex.Message);
                    MessageBox.Show(errorMessage, "Error");
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

        private void btn_OpenXML_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog opn = new FolderBrowserDialog();
            if (opn.ShowDialog() == DialogResult.OK)
            {
                try
                {
                    tbox_DataPath.Text = opn.SelectedPath;
                    //string[] workfiles = Utils.GetFiles(opn.SelectedPath);
                    //foreach (string element in workfiles)
                    refreshFile();
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Ошибка: Невозможно прочитать файл с диска. Текст ошибки: " + ex.Message);
                }
            }
        }

        private void btnRefresh_Click(object sender, EventArgs e)
        {
            refreshFile();
        }

        private void refreshFile()
        {
            clb_Files.Items.Clear();
            clb_FileOK.Items.Clear();
            workfiles = Utils.GetShortFiles(tbox_DataPath.Text);
            foreach (KeyValuePair<string, string> element in workfiles)
            {
                clb_Files.Items.Add(element.Key, false);
                clb_FileOK.Items.Add("Требует проверки", false);
            }
        }

        //private void FiasUpload(string xmlpath, string xsdpath)
        //{
        //    DateTime dt = DateTime.Parse("1900-01-01");
        //    string tab_name = xmlpath.Substring(xmlpath.LastIndexOf("\\") + 4, xmlpath.LastIndexOf(".") - xmlpath.LastIndexOf("\\") - 4);

        //    Dictionary<string, string> ConnectSettings = new Dictionary<string, string>();

        //    ConnectSettings["Server"] = tbox_Srv.Text.Trim();
        //    ConnectSettings["DB"] = tbox_DB.Text.Trim();
        //    ConnectSettings["User"] = tbox_Login.Text.Trim();
        //    ConnectSettings["Password"] = tbox_Pass.Text.Trim();

        //    try
        //    {
        //        dt = Convert.ToDateTime(tbDateVersion.Text);
        //        MyXmlReader xml = new MyXmlReader(xmlpath, xsdpath, ConnectSettings, 500);

        //        //создать??? или очистить _NEW таблицы???
        //        xml.DoRead(dt);

        //        //MyDB DBProc = new MyDB(ConnectSettings);
        //        //DBProc.Merge(tab_name);


        //        //DBProc.WriteVersion(tbDateVersion.Text);

        //        //дописать фильтр по региону
        //    }
        //    catch (Exception ex)
        //    {
        //        MessageBox.Show("Произошла ошибка, подробнее в логе");
        //        Logs.AppendText(ex.Message);
        //        Logs.AppendText("\r\n");
        //    }    
        //}

        private void btnSearch_Click(object sender, EventArgs e)
        {
            dgvTab.Rows.Clear();
            FiasSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
        }

        private void dgvTab_CellContentDoubleClick(object sender, DataGridViewCellEventArgs e)
        {
            if (e.ColumnIndex == 6)
            {
                string str = dgvTab[e.ColumnIndex, e.RowIndex].Value.ToString();
                dgvTab.Rows.Clear();
                FiasSearch(str.Remove(str.LastIndexOf(",")), "", cbOnlyAct.Checked);
            }
        }

        private void FiasSearch(string elemet, string parent, bool isAct)
        {
            Dictionary<string, string> ConnectSettings = new Dictionary<string, string>();

            ConnectSettings["Server"] = tbox_Srv.Text.Trim();
            ConnectSettings["DB"] = tbox_DB.Text.Trim();
            ConnectSettings["User"] = tbox_Login.Text.Trim();
            ConnectSettings["Password"] = tbox_Pass.Text.Trim();

            //if (tbox_Srv.Text == "" || tbox_DB.Text == "" || tbox_Login.Text == "" || tbox_Pass.Text == "")


            DataTable Table = new DataTable();

            MyDB Elemet = new MyDB(ConnectSettings);
            Table = Elemet.GetElement(elemet, parent, isAct);

            int columnCount = dgvTab.ColumnCount;
            string[] rowData = new string[columnCount];

            foreach (DataRow row in Table.Rows)
            {
                for (int k = 0; k < columnCount; k++)
                {
                    rowData[k] = row[k].ToString();
                }

                dgvTab.Rows.Add(rowData);
            }
        }

        private void main_form_Shown(object sender, EventArgs e)
        {
            //временно
            tbox_Srv.Text = "172.16.2.7";
            tbox_DB.Text = "fias";
            tbox_Login.Text = "dba";
            tbox_Pass.Text = "admin";

            tbox_Soap.Text = "http://fias.nalog.ru/WebServices/Public/DownloadService.asmx";
            tbox_Proxy.Text = "http://10.4.145.47";
            tbox_PPort.Text = "8080";

            tbox_DataPath.Text = "D:\\Downloads\\fias_delta_xml";
            tbox_SchemePath.Text = "D:\\work\\fias_xml\\XSD";
        }

        private void btnManualLoad_Click(object sender, EventArgs e)
        {
            Dictionary<string, string> ConnectSettings = new Dictionary<string, string>();

            ConnectSettings["Server"] = tbox_Srv.Text.Trim();
            ConnectSettings["DB"] = tbox_DB.Text.Trim();
            ConnectSettings["User"] = tbox_Login.Text.Trim();
            ConnectSettings["Password"] = tbox_Pass.Text.Trim();


            //DateTime dt = DateTime.Parse("1900-01-01");

            //DateTime dt = DateTime.ParseExact( date[0], "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture);
            //не принимает пустую дату, точнее вызывается ошибка, исправить
            //dt = Convert.ToDateTime(tbDateVersion.Text);

            if (checkUpload())
            {
                foreach (object itemChecked in clb_Files.CheckedItems)
                {
                    LoadXMLInput input;
                    pbUpload.Value = 0;
                    string longName = workfiles[itemChecked.ToString()];
                    string shortName = itemChecked.ToString();

                    //string xmlpath = longName.Trim();
                    string xsdpath = shortName.Substring(shortName.LastIndexOf("\\"), shortName.LastIndexOf(".") - shortName.LastIndexOf("\\"));

                    if (tbDateVersion.Text == "" || tbUpdateVer.Text == "")
                    {
                        input = new LoadXMLInput(ConnectSettings, shortName, longName, tbox_SchemePath.Text + xsdpath + ".XSD", DateTime.Parse("1900-01-01"), 0, false);
                    }
                    else
                    {
                        input = new LoadXMLInput(ConnectSettings, shortName, longName, tbox_SchemePath.Text + xsdpath + ".XSD", Convert.ToDateTime(tbDateVersion.Text), Int32.Parse(tbUpdateVer.Text), true);
                    }

                    btnManualLoadXml.Enabled = false;
                    Text = "Загружаем ФИАС....";
                    bw.RunWorkerAsync(input);
                }
            }
        }

        //запуск проверки XML по XSD
        private void clb_Files_ItemCheck(object sender, ItemCheckEventArgs e)
        {
            if (cbCheckXML.Checked)
            {
                CheckedListBox listbox = (CheckedListBox)sender;
                if (!listbox.GetItemChecked(listbox.SelectedIndex) && !clb_FileOK.GetItemChecked(listbox.SelectedIndex)) //&& listbox.Tag == null
                {
                    string longName = workfiles[listbox.SelectedItem.ToString()];

                    //string filestr = listbox.SelectedItem.ToString().Trim();
                    //string xmlpath = filestr;
                    string xsdpath = longName.Substring(longName.LastIndexOf("\\"), longName.LastIndexOf(".") - longName.LastIndexOf("\\"));

                    if (Utils.isValidXML(longName, tbox_SchemePath.Text + xsdpath + ".XSD"))
                    {
                        clb_FileOK.SetItemChecked(listbox.SelectedIndex, true);
                        clb_FileOK.Items[listbox.SelectedIndex] = "Успешно поверен";
                    }
                    else
                    {
                        clb_FileOK.SetItemChecked(listbox.SelectedIndex, false);
                        clb_FileOK.Items[listbox.SelectedIndex] = "Ошибка валидации";
                    }
                }
            }
        }

        private void cbCheckXML_CheckedChanged(object sender, EventArgs e)
        {
            checkUpload();
        }

        private bool checkUpload()
        {
            bool ret = true;
            if (tbox_SchemePath.Text == "")
            {
                tbox_SchemePath.BackColor = Color.Red;
                cbCheckXML.Checked = false;
                ret = false;
            }
            else
            {
                tbox_SchemePath.BackColor = SystemColors.Window;
                ret = true;
            }
            return ret;
        }

        private void btnTest_Click(object sender, EventArgs e)
        {
            //MySoap req = new MySoap(tbox_Soap.Text.Trim());
            //req.ParseSoap();
            //MyXmlReader rr = new MyXmlReader(tbox_SchemePath.Text);
            //rr.
        }

        private void cbUseProxy_CheckedChanged(object sender, EventArgs e)
        {
            if (cbUseProxy.Checked)
            {
                tbox_Proxy.Enabled = true;
                tbox_PPort.Enabled = true;
                label10.Enabled = true;
                label11.Enabled = true;
            }
            else
            {
                tbox_Proxy.Enabled = false;
                tbox_PPort.Enabled = false;
                label10.Enabled = false;
                label11.Enabled = false;
            }
        }

        private void btnAutoUpdate_Click(object sender, EventArgs e)
        {
            Dictionary<string, string> ConnectSettings = new Dictionary<string, string>();

            ConnectSettings["Server"] = tbox_Srv.Text.Trim();
            ConnectSettings["DB"] = tbox_DB.Text.Trim();
            ConnectSettings["User"] = tbox_Login.Text.Trim();
            ConnectSettings["Password"] = tbox_Pass.Text.Trim();

            try
            {
                AutoUpdate au = new AutoUpdate(ConnectSettings, tbox_Soap.Text.Trim());
                //tbox_Soap
                //tbox_Proxy
                //tbox_PPort
                au.GetUpdateList();
                au.DownloadFiles();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Произошла ошибка, подробнее в логе");
                Logs.AppendText(ex.Message);
                Logs.AppendText("\r\n");
            }
        }

        private void tbSearch_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Return)
            {
                dgvTab.Rows.Clear();
                FiasSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
            }
        }

        private void tbParent_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Return)
            {
                dgvTab.Rows.Clear();
                FiasSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
            }
        }

        private void cbOnlyAct_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Return)
            {
                dgvTab.Rows.Clear();
                FiasSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
            }
        }

        private void btnHouseSearch_Click(object sender, EventArgs e)
        {
            dgvHouse.Rows.Clear();
            FiasHouseSearch(tbHouseSearch.Text.Trim(), tbHouseElm.Text.ToString(), tbHousePar.Text.Trim(), cbHouseOnlyAct.Checked);
        }

        private void FiasHouseSearch(string hnum, string elemet, string parent, bool isAct)
        {
            Dictionary<string, string> ConnectSettings = new Dictionary<string, string>();

            ConnectSettings["Server"] = tbox_Srv.Text.Trim();
            ConnectSettings["DB"] = tbox_DB.Text.Trim();
            ConnectSettings["User"] = tbox_Login.Text.Trim();
            ConnectSettings["Password"] = tbox_Pass.Text.Trim();

            //if (tbox_Srv.Text == "" || tbox_DB.Text == "" || tbox_Login.Text == "" || tbox_Pass.Text == "")


            DataTable Table = new DataTable();

            MyDB House = new MyDB(ConnectSettings);
            Table = House.GetHouse(hnum, elemet, parent, isAct);

            int columnCount = dgvTab.ColumnCount;
            string[] rowData = new string[columnCount];

            foreach (DataRow row in Table.Rows)
            {
                for (int k = 0; k < columnCount; k++)
                {
                    rowData[k] = row[k].ToString();
                }

                dgvHouse.Rows.Add(rowData);
            }
        }

        private void tbHouseSearch_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Return)
            {
                dgvHouse.Rows.Clear();
                //FiasHouseSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
            }
        }

        private void tbHouseElm_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Return)
            {
                dgvHouse.Rows.Clear();
                //FiasHouseSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
            }
        }

        private void tbHousePar_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Return)
            {
                dgvHouse.Rows.Clear();
                //FiasHouseSearch(tbSearch.Text.Trim(), tbParent.Text.Trim(), cbOnlyAct.Checked);
            }
        }

        private void btnLoadOKTMO_Click(object sender, EventArgs e)
        {
            int ExcelType = 0;
            OpenFileDialog opn = new OpenFileDialog();
            if (opn.ShowDialog() == DialogResult.OK)
            {
                try
                {
                    if (rbTypeMF.Checked)
                    {
                        ExcelType = 1;
                    }

                    if (rbTypeGKS.Checked)
                    {
                        ExcelType = 2;
                    }

                    ExcelPath = opn.FileName;
                    opn.Dispose();                

                    Dictionary<string, string> ConnectSettings = new Dictionary<string, string>();

                    ConnectSettings["Server"] = tbox_Srv.Text.Trim();
                    ConnectSettings["DB"] = tbox_DB.Text.Trim();
                    ConnectSettings["User"] = tbox_Login.Text.Trim();
                    ConnectSettings["Password"] = tbox_Pass.Text.Trim();

                    MyExcel excel = new MyExcel(ExcelPath);
                    excel.loadExcelFile(ExcelType);

                    //DataTable Table = new DataTable();

                    //MyDB Elemet = new MyDB(ConnectSettings);
                    //Table = Elemet.GetElement(elemet, parent, isAct);

                    //int columnCount = dgvTab.ColumnCount;
                    //string[] rowData = new string[columnCount];

                    //foreach (DataRow row in Table.Rows)
                    //{
                    //    for (int k = 0; k < columnCount; k++)
                    //    {
                    //        rowData[k] = row[k].ToString();
                    //    }

                    //    dgvTab.Rows.Add(rowData);
                    //}
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Произошла ошибка, подробнее в логе");
                    Logs.AppendText(ex.Message);
                    Logs.AppendText("\r\n");
                }
            }
        }

        private void btnLoadGIS_Click(object sender, EventArgs e)
        {

        }
    }
}
