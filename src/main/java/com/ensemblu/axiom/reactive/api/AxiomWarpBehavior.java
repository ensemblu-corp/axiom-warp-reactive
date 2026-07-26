package com.ensemblu.axiom.reactive.api;

import com.ensemblu.axiom.core.foundation.Nothing;

import java.time.Duration;


public  interface AxiomWarpBehavior extends WarpStrike  {
    WarpStrike withHistory(Duration lookback);

    Nothing shutdown();
}