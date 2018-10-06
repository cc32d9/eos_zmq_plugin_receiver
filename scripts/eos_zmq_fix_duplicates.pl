use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use LWP::UserAgent;

$| = 1;

my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';

my $url = 'http://10.0.3.51:8888';

my $ok = GetOptions
    ('dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'url=s'     => \$url);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 --acc=NAME --out=FILE.xlsx --start=YYYY-MM-DD [options...]\n",
    "The utility updates EOSIO_ACTIONS table and fixes duplicated records\n",
    "Options:\n",
    "  --dsn=DSN            \[$dsn\]\n",
    "  --dbuser=USER        \[$db_user\]\n",
    "  --dbpw=PASSWORD      \[$db_password\]\n",
    "  --url=URL            \[$url\]\n";
    exit 1;
}

my $json = JSON->new;

my $ua = LWP::UserAgent->new
    (keep_alive => 1,
     ssl_opts => { verify_hostname => 0 });
$ua->timeout(5);
$ua->env_proxy;

$url .= '/v1/chain/get_block';

my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;



my $sth_getdups = $dbh->prepare
    ('SELECT ' .
     ' id, block_num, trx_id ' .
     'FROM EOSIO_ACTIONS ' .
     'WHERE dup_seq=1 AND irreversible=1');


my $sth_upd = $dbh->prepare
    ('UPDATE EOSIO_ACTIONS SET irreversible=0 WHERE id=?');

$sth_getdups->execute();
my $rowcnt = 0;

my %tx_found;

while( my $row = $sth_getdups->fetchrow_hashref('NAME_lc') )
{
    my $block_num = $row->{'block_num'};
    
    if( not defined($tx_found{$block_num}) )
    {
        my $req = HTTP::Request->new( 'POST', $url );
        $req->header( 'Content-Type' => 'application/json' );
        
        $req->content($json->encode
                      ({'block_num_or_id' => $row->{'block_num'}}));
        my $response = $ua->request($req);
        
        if( not $response->is_success ) {
            die('Error while retrieving ' . $url . ' : ' . $response->decoded_content);
        }
        
        my $data = $json->decode($response->decoded_content);
    
        foreach my $entry (@{$data->{'transactions'}})
        {
            $tx_found{$block_num}{$entry->{'trx'}} = 1;
        }
    }

    if( not $tx_found{$block_num}{$row->{'trx_id'}} )
    {
        printf("Fixing id=%d\n", $row->{'id'});
        $sth_upd->execute($row->{'id'});
        $rowcnt++;
    }
}

$dbh->commit();
$dbh->disconnect();
print("Updated $rowcnt rows\n");
