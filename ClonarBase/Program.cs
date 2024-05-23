using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using _Column = Microsoft.SqlServer.Management.Smo.Column;
using _ForeignKey = Microsoft.SqlServer.Management.Smo.ForeignKey;
using _Index = Microsoft.SqlServer.Management.Smo.Index;
using _Table = Microsoft.SqlServer.Management.Smo.Table;

string databaseName = "FinxAgendamentoUatb";

string sourceConnectionString = $"";
string destinationConnectionString = $""; //connectionString sem "Database"
string destinationConnectionStringFull = $""; //connectionString com "Database"


// Criar e migrar banco de dados de origem
using (var sourceContext = new AppDbContext(sourceConnectionString))
{
    sourceContext.Database.Migrate();
}

// Replicar banco de dados
ReplicarBancoDeDados();

Console.WriteLine("Replicação concluída com sucesso.");

void ReplicarBancoDeDados()
{
    // Configurar conexão de origem
    var sourceConnection = new ServerConnection(new SqlConnection(sourceConnectionString));
    var sourceServer = new Server(sourceConnection);

    // Obter banco de dados de origem
    var sourceDatabase = sourceServer.Databases[databaseName];

    // Configurar conexão de destino
    var destinationConnection = new ServerConnection(new SqlConnection(destinationConnectionString));
    var destinationServer = new Server(destinationConnection);

    // Criar banco de dados de destino
    var destinationDatabase = new Database(destinationServer, databaseName);
    destinationDatabase.Create();

    // Replicar tabelas e dados
    foreach (_Table table in sourceDatabase.Tables)
    {
        if (table.IsSystemObject || table.IsExternal || table.Name.Contains("Backup") || table.Name.Contains("Temp") || table.Name.Contains("LogErroImportacaoCargaOLD") || table.Name.Contains("AuditRegistros")) continue;

        string schemaCheckQuery = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{table.Schema}'";
        int schemaExists = (int)destinationConnection.ExecuteScalar(schemaCheckQuery);

        // Se o esquema não existir, criar o esquema
        if (schemaExists == 0)
        {
            string createSchemaQuery = $"CREATE SCHEMA {table.Schema}";
            destinationDatabase.ExecuteNonQuery(createSchemaQuery);
        }

        var newTable = new _Table(destinationDatabase, table.Name, table.Schema);

        foreach (_Column column in table.Columns)
        {
            var newColumn = new _Column(newTable, column.Name, column.DataType)
            {
                Nullable = column.Nullable,
                Default = column.Default
            };

            // Copiar outras propriedades conforme necessário

            newTable.Columns.Add(newColumn);
        }

        Console.WriteLine($"Criando tabela {table.Schema}.{table.Name}...");
        newTable.Create();
    }

    CopyData();

    destinationDatabase = destinationServer.Databases[databaseName];
    //RemoveIndexesFromAllTables(destinationDatabase); Usado somente caso precise rodar novamente a criação dos indices
    UpdateKeys();
}

void CopyData2()
{
    var batchSize = 500;
    var lastProcessedTable = ""; // Variável para armazenar a última tabela processada em caso de timeout
    var continuar = false;

    // Configurar conexão de origem
    using (var sourceConnection = new SqlConnection(sourceConnectionString))
    {
        sourceConnection.Open();

        // Configurar conexão de destino
        using (var destinationConnection = new SqlConnection(destinationConnectionStringFull))
        {
            destinationConnection.Open();

            // Configurar conexão de origem
            var sourceServerConnection = new ServerConnection(new SqlConnection(sourceConnectionString));
            var sourceServer = new Server(sourceServerConnection);

            // Obter banco de dados de origem
            var sourceDatabase = sourceServer.Databases[databaseName];

            using (var transaction = destinationConnection.BeginTransaction())
            {
                try
                {
                    // Iterar sobre as tabelas
                    foreach (_Table table in sourceDatabase.Tables)
                    {
                        // Verificar se houve um timeout e a última tabela processada
                        //if (table.Name == lastProcessedTable)
                        //{
                        //    continuar = true;
                        //}

                        //if (!continuar) continue;

                        if (table.IsSystemObject || table.IsExternal || table.Name.Contains("Backup") || table.Name.Contains("Temp") || table.Name.Contains("LogErroImportacaoCargaOLD") || table.Name.Contains("AuditRegistros")) continue;

                        // Consulta SQL para selecionar dados da tabela de origem
                        string selectQuery = $"SELECT * FROM [{table.Schema}].[{table.Name}]";

                        // Comando para executar a consulta de seleção na base de origem
                        using (var selectCommand = new SqlCommand(selectQuery, sourceConnection))
                        {
                            using (var reader = selectCommand.ExecuteReader())
                            {
                                // Verifica se há mais linhas a serem processadas
                                if (!reader.HasRows) continue;

                                // Configurar o SqlBulkCopy
                                using (var bulkCopy = new SqlBulkCopy(destinationConnection, SqlBulkCopyOptions.Default, transaction))
                                {
                                    bulkCopy.DestinationTableName = $"{table.Schema}.{table.Name}";
                                    bulkCopy.BatchSize = batchSize;

                                    // Mapear as colunas
                                    foreach (_Column column in table.Columns)
                                    {
                                        bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                                    }

                                    // Copiar os dados em lote
                                    bulkCopy.WriteToServer(reader);
                                }
                            }
                        }
                        Console.WriteLine($"Copiando dados da tabela {table.Schema}.{table.Name}...");

                        // Atualizar a última tabela processada
                        lastProcessedTable = table.Name;

                        // Commit da transação se tudo ocorreu com sucesso
                        transaction.Commit();
                    }
                }
                catch (SqlException ex) when (ex.Number == -2) // Capturar apenas timeout
                {
                    // Rollback da transação em caso de timeout                
                    Console.WriteLine($"Timeout durante a cópia dos dados na tabela {lastProcessedTable}. Retomando de onde parou...");
                    CopyData2();
                }
                catch (Exception ex)
                {
                    // Rollback da transação em caso de erro
                    transaction.Rollback();
                    Console.WriteLine($"Erro durante a cópia dos dados: {ex.Message}");
                }
            }
        }
    }
}

void CopyData()
{
    var batchSize = 500;

    // Configurar conexão de origem
    using (var sourceConnection = new SqlConnection(sourceConnectionString))
    {
        sourceConnection.Open();

        // Configurar conexão de destino
        using (var destinationConnection = new SqlConnection(destinationConnectionStringFull))
        {
            destinationConnection.Open();

            // Configurar conexão de origem
            var sourceServerConnection = new ServerConnection(new SqlConnection(sourceConnectionString));
            var sourceServer = new Server(sourceServerConnection);

            // Obter banco de dados de origem
            var sourceDatabase = sourceServer.Databases[databaseName];

            using (var transaction = destinationConnection.BeginTransaction())
            {
                try
                {
                    foreach (_Table table in sourceDatabase.Tables)
                    {
                        if (table.IsSystemObject || table.IsExternal || table.Name.Contains("Backup") || table.Name.Contains("Temp") || table.Name.Contains("LogErroImportacaoCargaOLD") || table.Name.Contains("AuditRegistros")) continue;

                        // Consulta SQL para selecionar dados da tabela de origem
                        string selectQuery = $"SELECT * FROM [{table.Schema}].[{table.Name}]";

                        // Comando para executar a consulta de seleção na base de origem
                        using (var selectCommand = new SqlCommand(selectQuery, sourceConnection))
                        {
                            selectCommand.CommandTimeout = 120;
                            using (var reader = selectCommand.ExecuteReader())
                            {
                                // Verifica se há mais linhas a serem processadas
                                if (!reader.HasRows) continue;

                                // Configurar o SqlBulkCopy
                                using (var bulkCopy = new SqlBulkCopy(destinationConnection, SqlBulkCopyOptions.Default, transaction))
                                {
                                    bulkCopy.DestinationTableName = $"{table.Schema}.{table.Name}";
                                    bulkCopy.BatchSize = batchSize;

                                    // Mapear as colunas
                                    foreach (_Column column in table.Columns)
                                    {
                                        bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                                    }

                                    // Copiar os dados em lote
                                    bulkCopy.WriteToServer(reader);
                                }
                            }
                        }
                        Console.WriteLine($"Copiando dados da tabela {table.Schema}.{table.Name}...");
                    }

                    // Commit da transação se tudo ocorreu com sucesso
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    // Rollback da transação em caso de erro
                    transaction.Rollback();
                    Console.WriteLine($"Erro durante a cópia dos dados: {ex.Message}");
                }
            }
        }
    }
}

void UpdateKeys()
{
    // Configuração da conexão para o banco de origem
    var sourceConnection = new ServerConnection(new SqlConnection(sourceConnectionString));
    var sourceServer = new Server(sourceConnection);

    // Configuração da conexão para o banco de destino
    var destinationConnection = new ServerConnection(new SqlConnection(destinationConnectionStringFull));
    var destinationServer = new Server(destinationConnection);

    var sourceDatabase = sourceServer.Databases[databaseName];
    var destinationDatabase = destinationServer.Databases[databaseName];

    // Copie índices e PK
    foreach (_Table table in sourceDatabase.Tables)
    {
        if (table.IsSystemObject || table.IsExternal || table.Name.Contains("Backup") || table.Name.Contains("Temp") || table.Name.Contains("LogErroImportacaoCargaOLD") || table.Name.Contains("AuditRegistros")) continue;

        // Nome da tabela que será copiada
        string tableName = table.Name;

        // Esquema da tabela (por exemplo, "dbo")
        string tableSchema = table.Schema;

        // Obtenha a tabela do banco de origem
        _Table sourceTable = sourceDatabase.Tables[tableName, tableSchema];

        // Obtenha a tabela correspondente no banco de destino
        _Table destinationTable = destinationDatabase.Tables[tableName, tableSchema];

        if (sourceTable != null && destinationTable != null)
        {
            foreach (_Index sourceIndex in sourceTable.Indexes)
            {
                // Verificar se as colunas do índice existem na tabela de destino
                bool columnsExist = sourceIndex.IndexedColumns.Cast<IndexedColumn>().All(ic => destinationTable.Columns.Contains(ic.Name));

                if (columnsExist)
                {
                    // Criar um novo índice na tabela de destino com base nas propriedades do índice de origem
                    var destinationIndex = new _Index(destinationTable, sourceIndex.Name);
                    destinationIndex.IndexKeyType = sourceIndex.IndexKeyType;
                    destinationIndex.IndexType = sourceIndex.IndexType;

                    // Copiar outras propriedades conforme necessário

                    foreach (IndexedColumn sourceIndexedColumn in sourceIndex.IndexedColumns)
                    {
                        //Validação para ignorar criação de indices com colunas nvarchar(max)
                        if (ValidTypeIndex(sourceTable, sourceIndexedColumn) is false) continue;

                        // Verificar se a coluna existe na tabela de destino
                        _Column destinationColumn = destinationTable.Columns[sourceIndexedColumn.Name];

                        if (destinationColumn != null)
                        {
                            // Adicionar a coluna indexada ao índice da tabela de destino
                            destinationIndex.IndexedColumns.Add(new IndexedColumn(destinationIndex, destinationColumn.Name, sourceIndexedColumn.Descending));
                        }
                        else
                        {
                            Console.WriteLine($"A coluna {sourceIndexedColumn.Name} não existe na tabela de destino. Não foi possível adicionar ao índice.");
                        }
                    }

                    // Adicionar o índice à tabela de destino
                    destinationTable.Indexes.Add(destinationIndex);
                    destinationTable.Alter();

                    Console.WriteLine($"Criando índice {sourceIndex.Name} na tabela {tableSchema}.{tableName}...");
                }
                else
                {
                    Console.WriteLine($"Índice {sourceIndex.Name} não pode ser copiado para a tabela de destino. Algumas colunas não existem.");
                }
            }
        }
        else
        {
            Console.WriteLine("A tabela de origem ou destino não foi encontrada.");
        }
    }

    // Copie chaves estrangeiras
    foreach (_Table table in sourceDatabase.Tables)
    {
        if (table.IsSystemObject || table.IsExternal || table.Name.Contains("Backup") || table.Name.Contains("Temp") || table.Name.Contains("LogErroImportacaoCargaOLD") || table.Name.Contains("AuditRegistros")) continue;

        // Nome da tabela que será copiada
        string tableName = table.Name;

        // Esquema da tabela (por exemplo, "dbo")
        string tableSchema = table.Schema;

        // Obtenha a tabela do banco de origem
        _Table sourceTable = sourceDatabase.Tables[tableName, tableSchema];

        // Obtenha a tabela correspondente no banco de destino
        _Table destinationTable = destinationDatabase.Tables[tableName, tableSchema];

        if (sourceTable != null && destinationTable != null)
        {
            foreach (_ForeignKey foreignKey in sourceTable.ForeignKeys)
            {
                var newForeignKey = new _ForeignKey(destinationTable, foreignKey.Name);

                foreach (ForeignKeyColumn fkColumn in foreignKey.Columns)
                {
                    newForeignKey.Columns.Add(new ForeignKeyColumn(newForeignKey, fkColumn.Name, fkColumn.ReferencedColumn));
                }

                newForeignKey.ReferencedTable = foreignKey.ReferencedTable;
                newForeignKey.ReferencedTableSchema = foreignKey.ReferencedTableSchema;
                newForeignKey.DeleteAction = foreignKey.DeleteAction;
                newForeignKey.UpdateAction = foreignKey.UpdateAction;

                destinationTable.ForeignKeys.Add(newForeignKey);
                Console.WriteLine($"Criando ForeignKey {newForeignKey.Name} na tabela {tableSchema}.{tableName}...");
            }

            destinationTable.Alter();
        }
        else
        {
            Console.WriteLine("A tabela de origem ou destino não foi encontrada.");
        }
    }
}

static void RemoveIndexesFromAllTables(Database database)
{
    foreach (_Table table in database.Tables)
    {
        // Ignorar tabelas do sistema e externas, se necessário
        if (table.IsSystemObject || table.IsExternal || table.Name.Contains("Backup") || table.Name.Contains("Temp") || table.Name.Contains("LogErroImportacaoCargaOLD") || table.Name.Contains("AuditRegistros"))
            continue;

        // Remover índices existentes
        RemoveIndexesFromTable(table);
    }
}

static void RemoveIndexesFromTable(_Table table)
{
    var indexesCopy = new List<_Index>();
    foreach (_Index index in table.Indexes)
    {
        indexesCopy.Add(index);
    }

    foreach (_Index index in indexesCopy)
    {
        Console.WriteLine($"Removendo índice {index.Name} da tabela {table.Name}...");

        // Remover o índice
        index.DropIfExists();
    }

    // Alterar a tabela para aplicar as mudanças
    table.Alter();
}

static bool ValidTypeIndex(_Table sourceTable, IndexedColumn indexedColumn)
{
    _Column sourceColumn = sourceTable.Columns[indexedColumn.Name];

    if (sourceColumn != null)
    {
        // Obter o tipo de dado da coluna
        var dataType = sourceColumn.DataType;

        return dataType.SqlDataType != SqlDataType.NVarCharMax;
    }

    return false;
}