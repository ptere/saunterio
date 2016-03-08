<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"/>
    <meta name="viewport" content="width=device-width">
    <meta name="viewport" content="initial-scale=0.7">
    <title>Saunter &mdash; Nice weather alerts</title>
    <link href="/static/style.css" rel="stylesheet" type="text/css"/>
  </head>

  <body>
    <div id="boundingbox">

      % if eligible:
      <p class="maintext">{{scored_date_friendly}}&rsquo;s forecast score for Raleigh is</p>
      <div id="weatherscore">{{score}}</div>
      <p class="maintext">with the best weather from

        {{periods}}.<br/><br/>
        This score
        <b>
          % if beat_bool:
          beats
          % else:
          fails to beat
          % end
        </b>
        <!-- Long live the <b> tag. -->
        the day&rsquo;s threshold of</p>
      <div id="lastalert">{{threshold}}</div>
      <p class="maintext">to be considered a really nice day.</p>

      % else:
      <p class="maintext">{{scored_date_friendly}}&rsquo;s forecast for Raleigh<br/>
        % if past_tense:
        was not looking good as of last night.
        % else:
        is <a href="http://forecast.io/#/f/35.8738,-78.7912">not looking good</a>.
        % end
      </p>
      % end

      <p id="analysis">
        <br/>

        % if False: # disabled until a nice day comes along # not eligible or not beat_bool:
        % # I might instead say something like, "The threshold is crossed roughly once every three weeks. Learn more." and
        % # link to an about page from that.
        It has been
        {{days_since}}
        days since the forecast looked really good.<br/>
        % end
        <a href="/about">About</a><br/><br/> Updated nightly at 8:30pm ET
        <br/>
        Weather data kindly provided by
        <a href="https://forecast.io/">Forecast.io</a>.<br/><br/>
        <small>
          <span style="color:silver">Execution time:
            {{!data_prep_time}}</span>
        </small>
      </p>
    </div>
  </body>
</html>