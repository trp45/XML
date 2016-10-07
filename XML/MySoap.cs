using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using System.Xml;
using System.Xml.Linq;

namespace XML
{
    class MySoap
    {
        private string _webaddress;
        private string _proxy;
        private int _pport;

        public MySoap(string webaddress)
        {
            _webaddress = webaddress;
        }

        public MySoap(string webaddress, string proxy, int pport)
        {
            _webaddress = webaddress;
            _proxy = proxy;
            _pport = pport;
        }

        public Dictionary<int, string> ParseSoap(int dbver)
        {
            Dictionary<int, string> Ret = new Dictionary<int, string>();

            StringBuilder xml = new StringBuilder();
            xml.Append(@"<?xml version=""1.0"" encoding=""utf-8""?>");
            xml.Append(@"<soap:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" ");
            xml.Append(@"xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" ");
            xml.Append(@"xmlns:soap=""http://schemas.xmlsoap.org/soap/envelope/"">");
            xml.Append("<soap:Body>");
            xml.Append(@"<GetAllDownloadFileInfo xmlns=""");
            xml.Append(_webaddress);
            xml.Append(@""" />");
            xml.Append("</soap:Body>");
            xml.Append("</soap:Envelope>");

            XmlDocument soapEnvelopeXml = new XmlDocument();
            soapEnvelopeXml.LoadXml(xml.ToString());

            HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(_webaddress);
            if (_proxy != null) webRequest.Proxy = new WebProxy(_proxy, _pport);
            webRequest.Headers.Add(@"SOAP:Action");
            webRequest.ContentType = "text/xml;charset=\"utf-8\"";
            webRequest.Accept = "text/xml";
            webRequest.Method = "POST";

            using (Stream stream = webRequest.GetRequestStream())
            {
                soapEnvelopeXml.Save(stream);
            }

            using (WebResponse response = webRequest.GetResponse())
            {
                XDocument xdoc = XDocument.Load(response.GetResponseStream());
                
                XNamespace xname = _webaddress;

                //IEnumerable<XElement> elements = from elmts in xdoc.Descendants(xname + "DownloadFileInfo") select elmts;

                var elements = from elmts in xdoc.Descendants(xname + "DownloadFileInfo")
                            where ((int)elmts.Element(xname + "VersionId") > dbver)
                            orderby ((int)elmts.Element(xname + "VersionId"))
                            select elmts;


                foreach (var el in elements)
                {
                    //Console.WriteLine("Ver: {0}, URL: {1}", el.Element(aw + "VersionId").Value, el.Element(aw + "FiasDeltaXmlUrl").Value);
                    //yield return el.Element(xname + "VersionId").Value + el.Element(xname + "FiasDeltaXmlUrl").Value;
                    Ret.Add(Int32.Parse(el.Element(xname + "VersionId").Value), el.Element(xname + "FiasDeltaXmlUrl").Value);
                }
            }

            //    StreamWriter sw = new StreamWriter(request.GetRequestStream());
            //    sw.WriteLine(postData);
            //    sw.Close();
            return Ret;
        }

        public IEnumerable<string> ParseSoap2(string path)
        {
            XDocument xdoc = XDocument.Load(path);

            XNamespace aw = "http://fias.nalog.ru/WebServices/Public/DownloadService.asmx";

            IEnumerable<XElement> elements = from elmts in xdoc.Descendants(aw + "DownloadFileInfo") select elmts;

            foreach (XElement el in elements)
            {
                yield return el.Element(aw + "VersionId").Value + el.Element(aw + "FiasDeltaXmlUrl").Value;
            }
        }
    }
}
