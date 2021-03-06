use Test2::V0;
no warnings 'once';

my $ret= eval q{
	package test::Table1;
	use strict;
	use warnings;
	use parent 'DBIx::Class::Core';
	use DBIx::Class::ResultDDL qw( table col varchar );
	table 'table1';
	col c1 => varchar(10);
	1;
};
my $err= $@;
ok( $ret, 'eval Table1: simple exports' ) or diag $err;

$ret= eval q{
	package test::Table2;
	use DBIx::Class::ResultDDL -V0;
	BEGIN { $test::Table2::has_col_during_compile= __PACKAGE__->can('col') }
	$test::Table2::has_col_during_run= __PACKAGE__->can('col');
	1;
};
$err= $@;
ok( $ret, 'eval Table2: -V0' ) or diag $err;
ok( test::Table2->isa('DBIx::Class::Core'), 'DBIx Core added as parent' );
ok( $test::Table2::has_col_during_compile, 'added methods to namespace during compile' );
ok( !$test::Table2::has_col_during_run, 'removed methods from namespace after compile' );

# This proves that the sugar functions are removed before DBIC attempts to add
# the methods of that name to the Result class.
$ret= eval q{
	package test::Table3;
	use DBIx::Class::ResultDDL -V0;
	table 'table3';
	col col => integer;
	col default => varchar(50), null, default("foo");
	col char => char(1), null;
	primary_key 'col';
	1;
};
$err= $@;
ok( $ret, 'eval Table3: column conflicts with sugar function' ) or diag $err;

$ret= eval q{
  package MyApp::Schema::Result::Album;
  use DBIx::Class::ResultDDL qw/ -V1 /;
  table 'album';
  col artist_id => integer;

  package MyApp::Schema::Result::Artist;
  use DBIx::Class::ResultDDL qw/ -V1 -inflate_datetime /;
  
  table 'artist';
  col id           => integer unsigned auto_inc;
  col name         => varchar(25), null;
  col formed       => date;
  col disbanded    => date null;
  col general_info => json null;
  col last_update  => datetime('UTC');
  primary_key 'id';
  
  idx artist_by_name => [ 'name' ];
  
  has_many albums => { id => 'Album.artist_id' };
  rel_many impersonators => { name => 'Artist.name' };
  1;
};
$err= $@;
ok( $ret, 'Make sure the example in the Synopsis works, excluding inflate_json' ) or diag $err;

done_testing;
