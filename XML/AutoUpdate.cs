using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net;

namespace XML
{
    class AutoUpdate
    {
        private Dictionary<string, string> _connectSettings;
        private int _lastVer;
        private Dictionary<int, string> _updateFile;
        private string _webaddr;
        private string _proxy;
        private int _pport;

        public AutoUpdate(Dictionary<string, string> ConnectSettings, string webaddr)
        {
            _connectSettings = ConnectSettings;
            MyDB CurrVer = new MyDB(_connectSettings);
            _lastVer = CurrVer.ReadVersion();
            _webaddr = webaddr;

            //Вроде как уничтожит объект
            CurrVer = null;
        }

        public AutoUpdate(Dictionary<string, string> ConnectSettings, string webaddr, string proxy, int pport)
        {
            _connectSettings = ConnectSettings;
            MyDB CurrVer = new MyDB(_connectSettings);
            _lastVer = CurrVer.ReadVersion();
            _webaddr = webaddr;
            _proxy = proxy;
            _pport = pport;

            //Вроде как уничтожит объект
            CurrVer = null;
        }

        //TODO: Реализовать чтение версий и адресов из SOAP запроса,
        //в данном классе ответ нужно хранить, поочередно скачивать файлы и читать XML

        public void GetUpdateList()
        {
            MySoap request = new MySoap(_webaddr, _proxy, _pport);
            _updateFile = request.ParseSoap(_lastVer);
        }

        public void DownloadFiles()
        {
            string tempDir = Environment.GetEnvironmentVariable("TEMP");

            try
            {
                foreach (KeyValuePair<int, string> upfile in _updateFile)
                {
                    string pathString = Path.Combine(tempDir, "tmp_fias_" + upfile.Key.ToString());
                    if (Directory.Exists(pathString)) { Directory.Delete(pathString, true); }
                    Directory.CreateDirectory(pathString);

                    Uri address = new Uri(upfile.Value);
                    string fileName = Path.Combine(pathString, upfile.Value.Substring(upfile.Value.LastIndexOf("/")+1));

                    using (WebClient webCli = new WebClient())
                    {
                        if (_proxy != null) webCli.Proxy = new WebProxy(_proxy, _pport);
                        //webCli.DownloadFile(address, fileName);
                        webCli.DownloadProgressChanged += new DownloadProgressChangedEventHandler(DownloadProgressChanged);
                        webCli.DownloadFileAsync(address, fileName);
                    }
                }
            }
            catch (Exception ex)
            {
                String errorMessage;
                errorMessage = "Error: ";
                errorMessage = String.Concat(errorMessage, ex.Message);
                throw new Exception(errorMessage);
            }
        }

        private void DownloadProgressChanged(object sender, DownloadProgressChangedEventArgs e)
        {
            //TODO: Тут должно быть отображение загружаемого файла в прогресс бар
            //throw new NotImplementedException();
        }
    }
}
