using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace TestAPISharding.Models
{
    public class Navegacion : TableEntity
    {
        public int Id { get; set; }
        public string RFC { get; set; }
        public string Periodo { get; set; }
        public string Evento { get; set; }
        public string IdDeclaracion { get; set; }
        public DateTime HoraFecha { get; set; }
    }
}