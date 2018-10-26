using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;

namespace TestAPISharding.Controllers
{
    public class ShardingController : ApiController
    {
        public string Post(String id)
        {
            try
            {
                Random rnd = new Random();
                String rfc = $"{ObtenerLetraInicial(rnd.Next(1, 26))}ACX880101{id}";

                OperacionesStorage operacionesStorage = OperacionesStorage.ObtenerInstancia(rfc);
                return operacionesStorage.InsertarEntidad<ParametrosEstudio>(new ParametrosEstudio()
                {
                    EjecucionAlgoritmo = false,
                    Encuesta = "1",
                    Estudio = "1",
                    Grupo = "B1",
                    IdDeclaracion = id.ToString(),
                    Inicializada = false,
                    LogueoInicialEgresos = false,
                    LogueoInicialIngresos = false,
                    Override = "0",
                    PartitionKey = rfc,
                    Periodo = "009",
                    RowKey = id.ToString()
                });
            }
            catch (Exception ex)
            {
                return $"Mensaje{ex.Message}-stack{ex.StackTrace}";
            }
        }

        private string ObtenerLetraInicial(Int32 numero)
        {
            switch (numero)
            {
                case 1:
                    return "A";
                case 2:
                    return "B";
                case 3:
                    return "C";
                case 4:
                    return "D";
                case 5:
                    return "E";
                case 6:
                    return "F";
                case 7:
                    return "G";
                case 8:
                    return "H";
                case 9:
                    return "I";
                case 10:
                    return "J";
                case 11:
                    return "K";
                case 12:
                    return "L";
                case 13:
                    return "M";
                case 14:
                    return "N";
                case 15:
                    return "O";
                case 16:
                    return "P";
                case 17:
                    return "Q";
                case 18:
                    return "R";
                case 19:
                    return "S";
                case 20:
                    return "T";
                case 21:
                    return "U";
                case 22:
                    return "V";
                case 23:
                    return "W";
                case 24:
                    return "X";
                case 25:
                    return "Y";
            }

            return "Z";
        }

    }

    public class Balanceador
    {
        private static readonly Lazy<Balanceador> instancia = new Lazy<Balanceador>(() => new Balanceador());
        private static Dictionary<String, CloudTableClient> cadenasConexion;
        private static readonly String nombreSharding = "EstudioStorageConnectionStringShard";
        internal static Balanceador Instancia => instancia.Value;

        private Balanceador()
        {
        }

        internal CloudTableClient ObtenerClienteTabla(String rfc)
        {
            String primeraLetra = rfc.First().ToString().ToUpper();

            if (cadenasConexion == null)
            {
                cadenasConexion = new Dictionary<String, CloudTableClient>();
            }

            if (cadenasConexion.ContainsKey(primeraLetra) == false)
            {
                cadenasConexion.Add(primeraLetra,
                    CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting($"{nombreSharding}{primeraLetra}"))
                    .CreateCloudTableClient());
            }

            return cadenasConexion[primeraLetra];
        }
    }

    public sealed class OperacionesStorage
    {
        private static OperacionesStorage instancia;
        private String currentRfc;

        private OperacionesStorage()
        {
        }

        public static OperacionesStorage ObtenerInstancia(String rfc)
        {
            return new OperacionesStorage() { currentRfc = rfc };
        }

        public String InsertarEntidad<T>(T entidad, String nombreTabla = "") where T : TableEntity, new()
        {
            try
            {
                return this.CrearTableReference(String.IsNullOrEmpty(nombreTabla) == false ? nombreTabla : entidad.GetType().Name)
                .Execute(TableOperation.InsertOrReplace(entidad)).Etag.ToString();
            }
            catch (Exception e)
            {
                return $"Mensaje{e.Message}-stack{e.StackTrace}";
            }
        }

        public T RecuperarEntidad<T>(String partitionKey, String rowKey, String nombreTabla = "") where T : TableEntity, new()
        {
            try
            {
                var result = this.CrearTableReference(String.IsNullOrEmpty(nombreTabla) == false ? nombreTabla : typeof(T).Name)
                    .Execute(TableOperation.Retrieve<T>(partitionKey, rowKey));
                return (T)Convert.ChangeType(result.Result, typeof(T));
            }
            catch (Exception)
            {
                return null;
            }
        }

        public IEnumerable<T> RecuperarEntidades<T>(TableQuery<T> filtro, String nombreTabla = "") where T : TableEntity, new()
        {
            try
            {
                var result = this.CrearTableReference(String.IsNullOrEmpty(nombreTabla) == false ? nombreTabla : typeof(T).Name)
                    .ExecuteQuery(filtro);
                return result;
            }
            catch (Exception)
            {
                return null;
            }
        }

        private CloudTable CrearTableReference(String nombreTabla)
        {
            CloudTable tabla = Balanceador.Instancia.ObtenerClienteTabla(this.currentRfc).GetTableReference(nombreTabla);
            tabla.CreateIfNotExists();
            return tabla;
        }
    }

    public class ParametrosEstudio : TableEntity
    {
        public String Grupo { get; set; }
        public String Encuesta { get; set; }
        public String Override { get; set; }
        public String Estudio { get; set; }
        public String IdDeclaracion { get; set; }
        public String Periodo { get; set; }
        public Boolean EjecucionAlgoritmo { get; set; }
        public Boolean Inicializada { get; set; }
        public Boolean LogueoInicialIngresos { get; set; }
        public Boolean LogueoInicialEgresos { get; set; }

        public override string ToString()
        {
            return $"{PartitionKey}, {IdDeclaracion}";
        }
    }
}
