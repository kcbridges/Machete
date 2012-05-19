﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Machete.Domain;

namespace Machete.Service
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class dTableList<T>
    {
        public IEnumerable<T> query { get; set; }
        public int totalCount { get; set; }
        public int filteredCount { get; set; }
    }
}