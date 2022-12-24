package DBIx::Class::ResultDDL::SchemaLoaderMixin;
use strict;
use warnings;
use List::Util 'max', 'all';
use DBIx::Class::ResultDDL;
use Carp;
use Data::Dumper ();
sub deparse { Data::Dumper->new([$_[0]])->Terse(1)->Quotekeys(0)->Sortkeys(1)->Indent(0)->Dump }
use namespace::clean;

# ABSTRACT: Modify Schema Loader to generate ResultDDL notation
# VERSION

=head1 SYNOPSIS

  package MyLoader;
  use parent
    'DBIx::Class::ResultDDL::SchemaLoaderMixin', # mixin first
    'DBIx::Class::Schema::Loader::DBI::mysql';
  1;

  use DBIx::Class::Schema::Loader qw/ make_schema_at /;
  my %options= ...;
  my @conn_info= (
    'dbi:mysql:my_database',
    $user, $pass,
    { loader_class => 'MyLoader' }
  );
  make_schema_at($package, \%options, \@conn_info);

You can also use this custom loader class to inject some DBIC settings
that SchemaLoader doesn't know about:

  package MyLoader;
  use parent
    'DBIx::Class::ResultDDL::SchemaLoaderMixin', # mixin first
    'DBIx::Class::Schema::Loader::DBI::mysql';
  
  sub generate_resultddl_import_line {
    return "use DBIx::Class::ResultDDL qw/ -V2 -inflate_datetime -inflate_json /;\n"
  }
  
  sub generate_column_info_sugar {
    my ($self, $class, $colname, $colinfo)= @_;
    if ($colname eq 'jsoncol' || $colname eq 'textcol') {
      $colinfo->{serializer_class}= 'JSON'
    }
    $self->next::method($class, $colname, $colinfo);
  }
  1;

=head1 DESCRIPTION

This module overrides behavior of L<DBIx::Class::Schema::Loader::Base> to
generate Result files that use L<DBIx::Class::ResultDDL> notation.
C<< ::Schema::Loader::Base >> is the base class for all of the actual loader
classes, which are invoked by C<< ::Schema::Loader >> (but do not share a
class hierarchy with ::Schema::Loader itself).

This is essentially a Role, but Schema Loader isn't based on Moo(se) and this
ResultDDL distribution does not yet depend on Moo(se), so it just uses plain
perl multiple inheritance.  Inherit from the mixin first so that its methods
take priority.  (it does override private methods of schema loader, so without
the Role mechanism to verify it, there is a chance parts just stop working if
Schema Loader changes its internals.  But it's a development-time tool, and
you'll see the output change, and the output will still be valid)

=head1 METHODS

The following methods are public so that you can override them:

=head2 generate_resultddl_import_line

This should return a string like C<< "use DBIx::Class::ResultDDL qw/ -V2 /;\n" >>.
Don't forget the trailing semicolon.

=cut

#sub _write_classfile {
#   my ($self, $class, $text, $is_schema)= @_;
#   main::explain($class);
#   main::explain($text);
#   main::explain($self->{_dump_storage}{$class});
#   $self->next::method($class, $text, $is_schema);
#}

sub generate_resultddl_import_line {
	qq|use DBIx::Class::ResultDDL qw/ -V2 /;\n|
}

=head2 generate_column_info_sugar

  $perl_stmt= $loader->generate_column_info_sugar($class, $col_name, $col_info);

This runs for each column being generated on the result class.
It takes the name of the result class, the name of the column, and the hashref
of DBIC %col_info that ::Schema::Loader created.  It then returns the string of
C<$generated_perl> to appear in C<< "col $col_name => $generated_perl;\n" >>.

If you override this, you can use the class and column name to decide if you
want to alter the C<$col_info> before SchemaLoaderMixin works its magic.
For instance, you might supply datetimes or serializer classes that
::Schema::Loader wouldn't know you wanted.

You could also munge the returned string, or just create a string if your own.

=cut

sub generate_column_info_sugar {
	my ($self, $class, $col_name, $orig_col_info)= @_;

	my $checkpkg= $self->_get_class_check_namespace($class);
	my $class_settings= DBIx::Class::ResultDDL::_settings_for_package($checkpkg);

	my %col_info= %$orig_col_info;
	my $stmt= _get_data_type_sugar(\%col_info, $class_settings);
	$stmt .= ' null' if delete $col_info{is_nullable};
	$stmt .= ' default('.deparse(delete $col_info{default_value}).'),' if exists $col_info{default_value};
	# add sugar for inflate_json if the serializer class is JSON, but not if the package feature inflate_json
	# was enabled and the column type is flagged as json.
	$stmt .= ' inflate_json' if 'JSON' eq ($col_info{serializer_class}||'');
	
	# Test the syntax for equality to the original
	my $out;
	eval "package $checkpkg; \$out= DBIx::Class::ResultDDL::expand_col_options(\$checkpkg, $stmt);";
	defined $out or croak "Error verifying generated ResultDDL for $class $col_name: $@";
	
	if ($out->{'extra.unsigned'}) { $out->{extra}{unsigned}= delete $out->{'extra.unsigned'}; }

	# Ignore the problem where 'integer' generates a default size for mysql that wasn't
	# in the Schema Loader spec.  TODO: add an option to skip generating this.
	delete $out->{size} if $out->{size} && !$orig_col_info->{size};

	# Data::Dumper gets confused and thinks sizes need quoted
	if (defined $orig_col_info->{size} && $orig_col_info->{size} =~ /^[0-9]+$/) {
		$orig_col_info->{size}= 0 + $orig_col_info->{size};
	}

	if (deparse({ %col_info, %$out }) eq deparse({ %$orig_col_info })) {
		# Any field in %$out removes the need to have it in $col_info.
		# This happens with implied options like serializer_class => 'JSON'
		for (keys %col_info) {
			delete %col_info{$_} if exists $out->{$_};
		}
		# remove trailing comma
		$stmt =~ s/,\s*$//;
		# dump the rest, and done.
		$stmt .= ', '._maybe_quote_identifier($_).' => '.deparse($col_info{$_})
			for sort keys %col_info;
	}
	else {
		warn "Unable to use ResultDDL sugar '$stmt'\n  ".deparse({ %col_info, %$out })." ne ".deparse($orig_col_info)."\n";
		$stmt= join(', ', map _maybe_quote_identifier($_).' => '.deparse($orig_col_info->{$_}), sort keys %$orig_col_info);
	}
	return $stmt;
}

sub _dbic_stmt {
	my ($self, $class, $method)= splice(@_, 0, 3);
	$self->{_MyLoader_use_resultddl}{$class}++
		or $self->_raw_stmt($class, $self->generate_resultddl_import_line);
	if ($method eq 'table') {
		$self->_raw_stmt($class, q|table |.deparse(@_).';');
	}
	elsif ($method eq 'add_columns') {
		my @col_defs;
		while (@_) {
			my ($col_name, $col_info)= splice(@_, 0, 2);
			push @col_defs, [
				_maybe_quote_identifier($col_name),
				$self->generate_column_info_sugar($class, $col_name, $col_info)
			];
		}
		# align the definitions, but round up to help avoid unnecessary diffs
		# when new columns get added.
		my $widest= max map length($_->[0]), @col_defs;
		$widest= ($widest + 3) & ~3;
		$self->_raw_stmt($class, sprintf("col %-*s => %s;", $widest, @$_))
			for @col_defs;
	}
	elsif ($method eq 'set_primary_key') {
		$self->_raw_stmt($class, q|primary_key |.deparse(@_).';');
	}
	else {
		$self->next::method($class, $method, @_);
	}
	return;
}

my %data_type_sugar= (
	(map {
		my $type= $_;
		$type => sub { my ($col_info)= @_;
			if ($col_info->{size} && $col_info->{size} =~ /^[0-9]+$/) {
				return "$type(".delete($col_info->{size})."),";
			} elsif ($col_info->{size} && ref $col_info->{size} eq 'ARRAY'
				&& (1 <= scalar($col_info->{size}->@*) <= 2)
				&& (all { /^[0-9]+$/ } $col_info->{size}->@*)
			) {
				return "$type(".join(',', delete($col_info->{size})->@*)."),";
			} else {
				return $type;
			}
		}
	} qw( integer float real numeric decimal varchar char )),
	(map {
		my $type= $_;
		$type => sub { my ($col_info, $class_settings)= @_;
			# include timezone in type sugar, if known.
			if ($col_info->{timezone} && !ref $col_info->{timezone}) {
				return "$type(".deparse(delete $col_info->{timezone})."),";
			} else {
				return $type;
			}
		}
	} qw( datetime timestamp )),
	(map {
		my $type= $_;
		$type => sub { my ($col_info, $class_settings)= @_;
			# Remove serializer_class => 'JSON' if inflate_json is enabled package-wide
			delete $col_info->{serializer_class}
				if $class_settings->{inflate_json} && ($col_info->{serializer_class}||'') eq 'JSON';
			return $type;
		}
	} qw( json jsonb )),
);

sub _get_data_type_sugar {
	my ($col_info, $class_settings)= @_;

	my $t= delete $col_info->{data_type}
		or return ();

	my $pl= ($data_type_sugar{$t} //= do {
		my $sugar= DBIx::Class::ResultDDL->can($t);
		my @out= $sugar? $sugar->() : ();
		@out >= 2 && $out[0] eq 'data_type' && $out[1] eq $t? sub { $t }
		: sub { 'data_type => '.deparse($t).',' }
	})->($col_info, $class_settings);

	if ($col_info->{extra} && $col_info->{extra}{unsigned}) {
		$pl =~ s/,?$/,/ unless $pl =~ /\w$/;
		$pl .= ' unsigned';
		if (1 == keys %{ $col_info->{extra} }) {
			delete $col_info->{extra};
		} else {
			$col_info->{extra}= { %{ $col_info->{extra} } };
			delete $col_info->{extra}{unsigned};
		}
	}
	return $pl;
}

sub _maybe_quote_identifier {
	# TODO: complete support for perl's left-hand of => operator parsing rule
	$_[0] =~ /^[A-Za-z0-9_]+$/? $_[0] : deparse($_[0]);
}

our %per_class_check_namespace;
sub _get_class_check_namespace {
	my ($self, $class)= @_;
	return ($per_class_check_namespace{$class} ||= do {
		my $use_line= $self->generate_resultddl_import_line;
		local $DBIx::Class::ResultDDL::DISABLE_AUTOCLEAN= 1;
		my $pkg= 'DBIx::Class::ResultDDL_check' . scalar keys %per_class_check_namespace;
		my $perl= "package $pkg; $use_line 1";
		eval $perl or croak "Error setting up package to verify generated ResultDDL: $@\nFor code:\n$perl";
		$pkg;
	});
}

1;
