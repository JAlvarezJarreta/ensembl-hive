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


package Bio::EnsEMBL::Hive::Scripts::Hoover;

use strict;
use warnings;

use Bio::EnsEMBL::Hive::DBSQL::DBAdaptor;
use Bio::EnsEMBL::Hive::DBSQL::AnalysisJobAdaptor;

sub add_connection_command_line_options { 1 };

sub command_line_options {
    return [
                # specify the threshold datetime:
            'before_datetime=s'     => 'before_datetime',
            'days_ago=f'            => 'days_ago',
    ];
}


sub run {
    my $self = shift;

    my $threshold_datetime_expression;

    if(my $before_datetime = $self->param('before_datetime')) {
        $threshold_datetime_expression = "'$before_datetime'";
    } elsif(my $days_ago = $self->param('days_ago')) {
        $threshold_datetime_expression = "from_unixtime(unix_timestamp(now())-3600*24*$days_ago)";
    } else {
        die "Must provide either -before_datetime or -days_ago.\n";
    }

    my $sql = qq{
    DELETE j FROM job j
     WHERE j.status='DONE'
       AND j.when_completed < $threshold_datetime_expression
    };

    # FIXME: would be better to build a DBA
    my $dbc = $self->param('pipeline')->hive_dba->dbc();
    $dbc->do( $sql );

    # Remove the roles that are not attached to any jobs
    my $sql_roles = q{
    DELETE role
      FROM role LEFT JOIN job USING (role_id)
     WHERE job.job_id IS NULL
    };
    $dbc->do( $sql_roles );

    # Remove the workers that are not attached to any roles, but only the
    # ones that should actually have a role (e.g. have been deleted by the
    # above statement).
    my $sql_workers = q{
    DELETE worker
      FROM worker LEFT JOIN role USING (worker_id)
     WHERE role.role_id IS NULL AND work_done > 0
    };
    $dbc->do( $sql_workers );

    ## Remove old messages not attached to any jobs
    my $sql_log_message = qq{
    DELETE FROM log_message WHERE job_id IS NULL AND time < $threshold_datetime_expression
    };
    $dbc->do( $sql_log_message );

    ## Remove old analysis_stats
    my $sql_analysis_stats = qq{
    DELETE FROM analysis_stats_monitor WHERE time < $threshold_datetime_expression
    };
    $dbc->do( $sql_analysis_stats );
}


1;

__DATA__

=pod

=head1 NAME

    hoover_pipeline.pl

=head1 SYNOPSIS

    hoover_pipeline.pl {-url <url> | -reg_conf <reg_conf> -reg_alias <reg_alias>} [ { -before_datetime <datetime> | -days_ago <days_ago> } ]

=head1 DESCRIPTION

    hoover_pipeline.pl is a script used to remove old 'DONE' jobs from a continuously running pipeline database

=head1 USAGE EXAMPLES

        # delete all jobs that have been 'DONE' for at least a week (default threshold) :

    hoover_pipeline.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost:3306/lg4_long_mult"


        # delete all jobs that have been 'DONE' for at least a given number of days

    hoover_pipeline.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost:3306/lg4_long_mult" -days_ago 3


        # delete all jobs 'DONE' before a specific datetime:

    hoover_pipeline.pl -url "mysql://ensadmin:${ENSADMIN_PSW}@localhost:3306/lg4_long_mult" -before_datetime "2013-02-14 15:42:50"

=head1 OPTIONS

    -reg_conf <path>          : path to a Registry configuration file
    -reg_type <string>        : type of the registry entry ('hive', 'core', 'compara', etc - defaults to 'hive')
    -reg_alias <string>       : species/alias name for the Hive DBAdaptor
    -url <url string>         : url defining where hive database is located
    -nosqlvc <0|1>            : skip sql version check if 1
    -before_datetime <string> : delete jobs 'DONE' before a specific time
    -days_ago <num>           : delete jobs that have been 'DONE' for at least <num> days
    -h | -help                : show this help message

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

