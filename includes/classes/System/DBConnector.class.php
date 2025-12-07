<?php
declare(strict_types=1);
namespace System;
use PDO;
use PDOException;
use RuntimeException;

class DBConnector
{
  private string $user;
  private string $passwd;
  private PDO $db_conn;


  /**
  * @param string $driver   'pgsql' oder 'mysql'
  * @param string $database Name der Datenbank
  * @param string $role     'readonly', 'insertupdate', 'delete', 'auth'
  */

  public function __construct( string $driver, string $database, string $role )
  {
    switch( $role )
    {
      case 'readonly':
        $this->user = GetEnv( 'DB_READONLY_USER' ) ?: '';
        $this->passwd = GetEnv( 'DB_READONLY_PASS' ) ?: '';
        break;
      case 'insertupdate':
        $this->user = GetEnv( 'DB_UPDATE_USER' ) ?: '';
        $this->passwd = GetEnv( 'DB_UPDATE_PASS' ) ?: '';
        break;
      case 'delete':
        $this->user = GetEnv( 'DB_DELETE_USER' ) ?: '';
        $this->passwd = GetEnv( 'DB_DELETE_PASS' ) ?: '';
        break;
    case 'auth':
        $this->user = GetEnv( 'DB_AUTH_USER' ) ?: '';
        $this->passwd = GetEnv( 'DB_AUTH_PASS' ) ?: '';
        break;
      default:
        throw new \InvalidArgumentException("Nicht unterstützte Rolle: $role ('readonly', 'insertupdate', 'delete' oder 'auth').");
    }

    if ($this->user === '' || $this->passwd === '') {
      throw new RuntimeException( "DB-Credentials für Rolle '{$role}' sind nicht vollständig gesetzt (ENV-Variablen fehlen?)." );
    }

    $driver = strtolower( $driver );

    switch( $driver )
    {
      case 'pgsql':
        $dsn = "pgsql:host=".GetEnv( 'DB_HOST' ).";port=".GetEnv( 'DB_PORT_PGSQL' ).";dbname={$database}";
        break;
      case 'mysql':
        $dsn = "mysql:host=".GetEnv( 'DB_HOST' ).";port=".GetEnv( 'DB_PORT_MYSQL' ).";dbname={$database};charset=utf8mb4"; 
        break;
      default:
        throw new \InvalidArgumentException("Nicht unterstützter Treiber: $driver ('pgsql' oder 'mysql').");
        break;
    }

    try
    {
      $this->db_conn = new PDO
      ( 
        $dsn, 
        $this->user, 
        $this->passwd,
        [
          PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
          PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
          PDO::ATTR_EMULATE_PREPARES   => false,
          PDO::ATTR_CASE               => PDO::CASE_NATURAL,
        ] 
      );
    }
    catch( PDOException $e )
    {
      throw new \RuntimeException( 'Datenbankverbindung fehlgeschlagen: ' . $e->getMessage() );
    }
  }

  public function getConnection(): PDO
  {
    return $this->db_conn;
  }

  public function fetchOne(string $sql, array $params = []): ?array
  {
    $stmt = $this->db_conn->prepare($sql);
    $stmt->execute($params);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
  }

  public function fetchAll(string $sql, array $params = []): array
  {
    $stmt = $this->db_conn->prepare($sql);
    $stmt->execute($params);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
  }

  public function execute(string $sql, array $params = []): int
  {
    $stmt = $this->db_conn->prepare($sql);
    $stmt->execute($params);
    return $stmt->rowCount();
  }

  public function fetchColumn(string $sql, array $params = []): mixed
  {
    $stmt = $this->db_conn->prepare($sql);
    $stmt->execute($params);
    return $stmt->fetchColumn();
  }

  public function begin(): void
  {
    $this->db_conn->beginTransaction();
  }

  public function commit(): void
  {
    $this->db_conn->commit();
  }

  public function rollback(): void
  {
    $this->db_conn->rollBack();
  }
}




?>