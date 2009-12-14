<div id="stuff" style="display: none;">
<?php
require_once 'json.php';
$count_json = file_get_contents("display.json");
$counts = json_decode($count_json);
$c = $_REQUEST["c"];
$total = 3;
if($c % $total == 0) {
    ?> 
    <h2>Completed Jobs Reported Per Minute in the Last 24 Hours</h2>
    <object id="chart" data="http://t2.unl.edu/store/pr_display/jobs.svg" width="100%" height="100%" type="image/svg+xml" codebase="http://www.adobe.com/svg/viewer/install/" /> 
Each finished job on the OSG is recorded into the accounting system. Over <?=number_format($counts->num_jobs)?> jobs were run by <?=number_format($counts->num_users)?> unique users in the last 24 hours.
    <?
} else if($c % $total == 2) {
    ?> 
    <h2>Users Using the OSG in the Last 24 Hours</h2>
    <object id="chart" data="http://t2.unl.edu/store/pr_display/users.svg" width="100%" height="100%" type="image/svg+xml" codebase="http://www.adobe.com/svg/viewer/install/" /> 
The number of users is recorded into the accounting system for each hour. There were <?=number_format($counts->num_users)?> distinct users were recorded in the last 24 hours.
        <?
} else if($c % $total == 1) {
    ?> 
    <h2>Average Transfer Rate (Gigabytes/s) in the Last 24 Hours</h2>
    <object id="chart" data="http://t2.unl.edu/store/pr_display/transfers.svg" width="100%" height="100%" type="image/svg+xml" codebase="http://www.adobe.com/svg/viewer/install/" /> 
Completed transfer at many OSG sites are reported to the accounting system. Over <?=number_format($counts->num_transfers)?> transfers were reported in the last 24 hours.
    <?
} else if ($c % $total == 3) {
    ?>
<h2 class="round">In the <span id="content_since">0</span> Seconds Since this Page Loaded, we estimate...</h2>
<ul>
<li><span class="count round4" id="content_jobs"><?=number_format($counts->num_transfers)?></span>&nbsp;Jobs have been completed</li>
<li><span class="count round4" id="content_trans"><?=number_format($counts->num_transfers)?></span>&nbsp;Transfers have occurred</li>
<li><span class="count round4" id="content_volume"><?=number_format($counts->transfer_volume_mb)?></span>&nbsp;MB have been transferred</li>
</ul>
<br/>

<br/>

<script type="text/javascript">
var content_jobs = 0;
var content_trans = 0;
var content_volume = 0;
var content_since = 0;

function increment_content_since()
{
    content_since+=.1;
    $("#content_since").html(addCommas(content_since.toFixed(1)));
    setTimeout(increment_content_since, 100);
}
function increment_content_jobs()
{
    content_jobs+=<?=$counts->jobs_rate/10?>;
    $("#content_jobs").html(addCommas(Math.round(content_jobs)));
    setTimeout(increment_content_jobs, 100);
}
function increment_content_trans()
{
    content_trans+=<?=$counts->transfer_rate/10?>;
    $("#content_trans").html(addCommas(Math.round(content_trans)));
    setTimeout(increment_content_trans, 100);
}
function increment_content_volume()
{
    content_volume+=<?=$counts->transfer_volume_rate/10?>;
    $("#content_volume").html(addCommas(Math.round(content_volume)));
    setTimeout(increment_content_volume, 100);
}
$(document).ready(function() {
    setTimeout(increment_content_jobs, 0);
    setTimeout(increment_content_trans, 0);
    setTimeout(increment_content_volume, 0);
    setTimeout(increment_content_since, 0);
});
function addCommas(nStr)
{
    nStr += '';
    x = nStr.split('.');
    x1 = x[0];
    x2 = x.length > 1 ? '.' + x[1] : '';
    var rgx = /(\d+)(\d{3})/;
    while (rgx.test(x1)) {
        x1 = x1.replace(rgx, '$1' + ',' + '$2');
    }
    return x1 + x2;
}

</script>

    <?
    }
    ?>

</div>
