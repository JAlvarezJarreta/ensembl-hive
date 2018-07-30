=pod 

=head1 NAME

    Bio::EnsEMBL::Hive::Role

=head1 DESCRIPTION

    Role is a state of a Worker while performing jobs of a particular Analysis.

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2019] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=head1 APPENDIX

    The rest of the documentation details each of the object methods.
    Internal methods are usually preceded with a _

=cut


package Bio::EnsEMBL::Hive::Role;

use strict;
use warnings;

use POSIX;

use Math::Gauss;

use base ( 'Bio::EnsEMBL::Hive::Storable' );


=head1 AUTOLOADED

    worker_id / worker
    analysis_id / analysis

=cut


sub when_started {
    my $self = shift;
    $self->{'_when_started'} = shift if(@_);
    return $self->{'_when_started'};
}


sub seconds_since_when_started {
    my $self = shift;
    $self->{'_seconds_since_when_started'} = shift if(@_);
    return $self->{'_seconds_since_when_started'};
}


sub when_finished {
    my $self = shift;
    $self->{'_when_finished'} = shift if(@_);
    return $self->{'_when_finished'};
}


sub attempted_jobs {
    my $self = shift;
    $self->{'_attempted_jobs'} = shift if(@_);
    return $self->{'_attempted_jobs'} || 0;
}


sub done_jobs {
    my $self = shift;
    $self->{'_done_jobs'} = shift if(@_);
    return $self->{'_done_jobs'} || 0;
}


# Estimate how much time is needed for the current job
sub _estimate_current_job_runtime_msec {
    my $self           = shift;
    my $analysis_stats = shift;

    # Guaranteed to be non-zero
    my $avg_msec_per_job = $analysis_stats->avg_msec_per_job // $analysis_stats->min_job_runtime_msec;

    # Will hold how time has been spent on the current job
    my $runtime_msec_of_current_job = $self->seconds_since_when_started * 1000;
    if ($self->attempted_jobs) {
        # The role has already attempted several jobs. We need to subtract that
        $runtime_msec_of_current_job -= $avg_msec_per_job * $self->attempted_jobs;
        # Can be negative if the jobs were faster than the average
        $runtime_msec_of_current_job = 0 if $runtime_msec_of_current_job < 0;
    }

    # The job hasn't started yet
    return $avg_msec_per_job unless $runtime_msec_of_current_job;

    # Job runtimes are modelled by a log-normal distribution
    # Empirical data show that sigma itself follows a log-normal distribution (-0.04219451,0.62547091) so we take the median
    my $sigma = exp(-0.04219451);
    # $avg_msec_per_job is the average job runtime
    my $mu = log($avg_msec_per_job) - $sigma**2/2;

    # This is E[X | X > $runtime_msec_of_current_job] * P(X > $runtime_msec_of_current_job)
    # (see https://en.wikipedia.org/wiki/Log-normal_distribution#Partial_expectation)
    my $k = $runtime_msec_of_current_job;
    my $ecp = $avg_msec_per_job * cdf(($mu + $sigma**2 - log($k)) / $sigma);
    # And this is is P(X > $runtime_msec_of_current_job) using:
    # # P(X > $runtime_msec_of_current_job) = 1 - P(X <= $runtime_msec_of_current_job)
    # # https://en.wikipedia.org/wiki/Log-normal_distribution#Cumulative_distribution_function
    # # the symmetry of cdf around 0, i.e. cdf(x) + cdf(-x) = 1
    my $cp = cdf(-(log($k) - $mu) / $sigma);
    my $remaining_runtime_msec_of_current_job = $ecp / $cp;
    return $remaining_runtime_msec_of_current_job;
}


# Estimate how many jobs extra jobs this role can take in the next $interval_msec
sub _estimate_number_job_attempts {
    my $self           = shift;
    my $analysis_stats = shift;
    my $interval_msec  = shift;

    # Guaranteed to be non-zero
    my $avg_msec_per_job = $analysis_stats->avg_msec_per_job // $analysis_stats->min_job_runtime_msec;

    my $remaining_runtime_msec_of_current_job = $self->_estimate_current_job_runtime_msec($analysis_stats);

    if ($remaining_runtime_msec_of_current_job > $interval_msec) {
        return 0;
    } else {
        # We acknowledge that the current job will take a bit longer, and
        # we fall back to $avg_msec_per_job for the rest of the interval
        return POSIX::ceil(($interval_msec - $remaining_runtime_msec_of_current_job) / $avg_msec_per_job);
    }
}


sub register_attempt {
    my $self    = shift;
    my $success = shift;

    $self->{'_attempted_jobs'}++;
    $self->{'_done_jobs'}     += $success;

    if( my $adaptor = $self->adaptor ) {
        $adaptor->update_attempted_jobs_AND_done_jobs( $self );
    }
}

1;
