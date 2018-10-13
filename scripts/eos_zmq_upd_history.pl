use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;

$| = 1;

my $rescan;

my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';

my $json = JSON->new;

my $ok = GetOptions
    ('rescan'    => \$rescan,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility updates EOSIO_ACC_HISTORY table from raw transactions.\n",
    "Options:\n",
    "  --rescan             process the whole history from the beginning\n",
    "  --dsn=DSN            \[$dsn\]\n",
    "  --dbuser=USER        \[$db_user\]\n",
    "  --dbpw=PASSWORD      \[$db_password\]\n" ;
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;


my $seq = 0;
if( not $rescan )
{
    my $r = $dbh->selectall_arrayref('SELECT MAX(global_seq) FROM EOSIO_ACC_HISTORY');
    if( defined($r->[0][0]) )
    {
        $seq = $r->[0][0];
    }
}


my $sth_getactions = $dbh->prepare
    ('SELECT ' .
     ' global_action_seq as global_seq, block_num, ' .
     ' DATE(block_time) AS bd, block_time, actor_account AS contract, ' .
     ' action_name, trx_id, account_name ' .
     'FROM EOSIO_ACTIONS JOIN EOSIO_RESOURCE_BALANCES ON id=action_id ' .
     'WHERE global_action_seq > ? AND irreversible=1 AND status=0 ' .
     'ORDER BY global_action_seq LIMIT 10000');

my $sth_add_tx = $dbh->prepare
    ('INSERT INTO EOSIO_ACC_HISTORY ' .
     '(global_seq, block_num, block_time, trx_id, ' .
     'account_name, contract, action_name) ' .
     'VALUES(?,?,?,?,?,?,?)');

my @add_tx_columns =
    qw(global_seq block_num block_time trx_id account_name contract action_name);

my $processing_date = '';
my $rowcnt = 0;

while(1)
{
    $sth_getactions->execute($seq);

    my $r = $sth_getactions->fetchall_arrayref({});
    my $nrows = scalar(@{$r});
    if( $nrows == 0 )
    {
        last;
    }
    
    foreach my $row (@{$r})
    {
        $seq = $row->{'global_seq'};
        
        if( $row->{'bd'} ne $processing_date )
        {
            $processing_date = $row->{'bd'};
            print("Processing $processing_date\n");
        }

        $sth_add_tx->execute
            (map {$row->{$_}} @add_tx_columns);
        $rowcnt++;
    }        
    
    $dbh->commit();
    print("$rowcnt\n");
}


$dbh->disconnect();

print("Finished\n");

