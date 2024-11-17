LISTINGS_LIST_GRAPHQL_QUERY = """
query getListingData($path: String!) {
  breadcrumbs(path: $path) {
    label
    tracking
    uri
    noAppend
    i18nLabelKey
  }
  redirects(path: $path) {
    code
    redirectUri
  }
  seoAccordions(path: $path) {
    ...SEO_ACCORDIONS_FRAGMENT
  }
  searchResults(path: $path) {
    analyticsTaxonomy {
      ...ANALYTICS_TAXONOMY_FRAGMENT
    }
    analyticsEcommerce {
      ...ANALYTICS_ECCOMERCE_FRAGMENT
    }
    adTargeting {
      ...AD_TARGETING_FRAGMENT
    }
    metaInfo {
      ...META_INFO_FRAGMENT
    }
    pagination {
      ...PAGINATION_FRAGMENT
    }
    listings {
      regular {
        ...LISTING_FRAGMENT
      }
      extended {
        ...LISTING_FRAGMENT
      }
      featured {
        ...LISTING_FRAGMENT
      }
    }
    links {
      ...LINKS_FRAGMENT
    }
    seoBlurb {
      ...SEO_BLURB_FRAGMENT
    }
    title
    userAlertId
    savedSearchAndAlerts {
      ...SAVED_SEARCH_AND_ALERTS_FRAGMENT
    }
    polyenc
  }
}

fragment SEO_ACCORDIONS_FRAGMENT on SeoAccordions {
  category
  expanded
  geo
  name
  propertyType
  section
  transactionType
  rows {
    links {
      category
      geo
      propertyType
      section
      transactionType
      uri
    }
  }
  links {
    category
    geo
    propertyType
    section
    transactionType
    uri
  }
}

fragment ANALYTICS_TAXONOMY_FRAGMENT on AnalyticsTaxonomy {
  url
  areaName
  activity
  brand
  countryCode
  countyAreaName
  currencyCode
  listingsCategory
  outcode
  outcodes
  page
  postalArea
  radius
  radiusAutoexpansion
  regionName
  resultsSort
  searchGuid
  searchIdentifier
  section
  searchLocation
  viewType
  searchResultsCount
  expandedResultsCount
  totalResults
  emailAllFormShown
  emailAllTotalAgents
  bedsMax
  bedsMin
  priceMax
  priceMin
  location
  propertyType
  geoIdentifier
}

fragment ANALYTICS_ECCOMERCE_FRAGMENT on AnalyticsEcommerce {
  currencyCode
  impressions {
    id
    list
    position
    variant
  }
}

fragment AD_TARGETING_FRAGMENT on AdTargeting {
  activity
  areaName
  bedsMax
  bedsMin
  brand
  brandPrimary
  countyAreaName
  countryCode
  currencyCode
  listingsCategory
  outcode
  outcodes
  page
  postalArea
  priceMax
  priceMin
  propertyType
  regionName
  resultsSort
  searchLocation
  searchResultsCount
  section
  totalResults
  url
  viewType
}

fragment META_INFO_FRAGMENT on Meta {
  title
  description
  canonicalUri
}

fragment PAGINATION_FRAGMENT on Pagination {
  pageNumber
  pageNumberMax
  totalResults
  totalResultsWasLimited
}

fragment LISTING_FRAGMENT on Listing {
  numberOfVideos
  numberOfImages
  numberOfFloorPlans
  numberOfViews
  listingId
  title
  publishedOnLabel
  publishedOn
  availableFrom
  priceDrop {
    lastPriceChangeDate
    percentageChangeLabel
  }
  isPremium
  highlights {
    description
    label
    url
  }
  branch {
    name
    branchDetailsUri
    phone
    logoUrl
    branchId
  }
  features {
    content
    iconId
  }
  image {
    src
    caption
    responsiveImgList {
      width
      src
    }
  }
  transports {
    title
    poiType
    distanceInMiles
    features {
      zone
      tubeLines
    }
  }
  flag
  listingId
  priceTitle
  price
  alternativeRentFrequencyLabel
  address
  tags {
    content
  }
  listingUris {
    contact
    detail
  }
  isNewHomesDevelopment
  summaryDescription
}

fragment LINKS_FRAGMENT on Links {
  saveSearch
  createAlert
}

fragment SEO_BLURB_FRAGMENT on SeoBlurb {
  category
  transactionType
}

fragment SAVED_SEARCH_AND_ALERTS_FRAGMENT on SavedSearchAndAlerts {
  uri
  currentFrequency {
    i18NLabelKey
    value
  }
}
"""