=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2017] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

package Bio::EnsEMBL::Hive::Scripts::DbCmd;

use strict;
use warnings;

use Bio::EnsEMBL::Hive::DBSQL::DBAdaptor;

use base ('Bio::EnsEMBL::Hive::Scripts::BaseScript');


sub add_connection_command_line_options { 1 };
sub must_use_all_command_line_options { 0 };


sub command_line_options {
    return [
            'exec|executable=s' => 'executable',
            'prepend=s@'        => 'prepend',
            'append|extra=s@'   => 'append',
            'sqlcmd|sql=s'      => 'sqlcmd',

            # FIXME unify as a "debug" option ?
            'verbose!'          => 'verbose',
        ];
}



sub run {
    my $self = shift;

    if (@{$self->param('append')}) {
        warn qq{In db_cmd.pl, final arguments don't have to be declared with --append any more. All the remaining arguments are considered to be appended.\n};
    }

    # FIXME: $dbc is not yet defined. The base-class only knows about
    # building a HivePipeline. This should be extended.
    my @cmd = @{ $dbc->to_cmd( $self->param('executable'), $self->param('prepend'), [@{$self->param('append')}, @ARGV], $self->param('sqlcmd') ) };
    $dbc->disconnect_if_idle;

    if( $self->param('verbose') ) {
        my $flat_cmd = join(' ', map { ($_=~/^-?\w+$/) ? $_ : "\"$_\"" } @cmd);

        warn "\nThe actual command I am running is:\n\t$flat_cmd\n\n";
    }

    exec(@cmd);
}

1;

__DATA__

=pod

=head1 NAME

    db_cmd.pl

=head1 SYNOPSIS

    db_cmd.pl {-url <url> | [-reg_conf <reg_conf>] -reg_alias <reg_alias> [-reg_type <reg_type>] } [ -exec <alt_executable> ] [ -prepend <prepend_params> ] [ -sql <sql_command> ] [ -verbose ] [other arguments to append to the command line]

=head1 DESCRIPTION

    db_cmd.pl is a generic script that connects you interactively to your database using either URL or Registry and optionally runs an SQL command.
    -url is exclusive to -reg_alias. -reg_type is only needed if several databases map to that alias / species.
    If the arguments that have to be appended contain options (i.e. start with dashes), first use a double-dash to indicate the end of db_cmd.pl's options and the start of the arguments that have to be passed as-is (see the example below with --html)

=head1 USAGE EXAMPLES

    db_cmd.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost:3306/" -sql 'CREATE DATABASE lg4_long_mult'
    db_cmd.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost:3306/lg4_long_mult"
    db_cmd.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost:3306/lg4_long_mult" -sql 'SELECT * FROM analysis_base' -- --html
    db_cmd.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost/lg4_long_mult" -exec mysqldump -prepend -t analysis_base job

    db_cmd.pl -reg_conf ${ENSEMBL_CVS_ROOT_DIR}/ensembl-compara/scripts/pipeline/production_reg_conf.pl -reg_alias compara_master
    db_cmd.pl -reg_conf ${ENSEMBL_CVS_ROOT_DIR}/ensembl-compara/scripts/pipeline/production_reg_conf.pl -reg_alias mus_musculus   -reg_type core
    db_cmd.pl -reg_conf ${ENSEMBL_CVS_ROOT_DIR}/ensembl-compara/scripts/pipeline/production_reg_conf.pl -reg_alias squirrel       -reg_type core -sql 'SELECT * FROM coord_system'

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2017] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=cut

