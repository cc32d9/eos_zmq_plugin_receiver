use strict;
use warnings;
use Getopt::Long;
use DBI;

$| = 1;

my $connectstr = 'tcp://127.0.0.1:5556';
my $dsn = 'DBI:MariaDB:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';

my %blacklist = ('blocktwitter' => 1);
my @blacklist_acc;


my $ok = GetOptions
    ('connect=s' => \$connectstr,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'bl=s'      => \@blacklist_acc);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility removes spam transactions from the database\n",
    "Options:\n",
    "  --connect=ENDPOINT \[$connectstr\]\n",
    "  --dsn=DSN          \[$dsn\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n",
    "  --bl=ACCOUNT       \[blocktwitter\] blacklist one or more accounts\n";
    exit 1;
}

foreach my $bl ( @blacklist_acc )
{
    $blacklist{$bl} = 1;
}
        

my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;

my $sth_find = $dbh->prepare
    ('SELECT global_action_seq FROM EOSIO_ACTIONS ' .
     'WHERE actor_account IN (' .
     join(',', map {'\'' . $_ . '\''} keys %blacklist) . ') LIMIT 1000');

my $sth_del_act = $dbh->prepare
    ('DELETE FROM EOSIO_ACTIONS WHERE global_action_seq=?');

my $nrows = 0;

while(1)
{
    $sth_find->execute();
    my $r = $sth_find->fetchall_arrayref();
    last if( scalar(@{$r}) == 0 );
    
    foreach my $row (@{$r})
    {
        my $seq = $row->[0];
        $sth_del_act->execute($seq);
        $nrows++;
    }

    $dbh->commit();
    print $nrows, "\n";
}

print STDERR "Finished. Deleted $nrows rows;\n";
$dbh->disconnect();

