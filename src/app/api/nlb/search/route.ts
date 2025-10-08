// app/api/nlb/search/route.ts
import { NextRequest, NextResponse } from 'next/server';

// Rate limiting state (in production, use Redis or a proper rate limiter)
let lastRequestTime = 0;
const MIN_REQUEST_INTERVAL = 1100; // 1.1 seconds to be safe (1 req/sec limit)

// NLB API Configuration
const NLB_API_BASE = 'https://openweb.nlb.gov.sg/api/v2/Catalogue';

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const query = searchParams.get('q') || '';
  const mediaCode = searchParams.get('mediaCode') || 'BOOK';
  const page = parseInt(searchParams.get('page') || '1', 10);
  const pageSize = parseInt(searchParams.get('pageSize') || '20', 10);
  
  const apiKey = process.env.NLB_API_KEY;

  if (!apiKey) {
    return NextResponse.json(
      { error: 'NLB API key not configured. Please add NLB_API_KEY to your .env.local file' },
      { status: 500 }
    );
  }

  if (!query) {
    return NextResponse.json(
      { error: 'Search query is required' },
      { status: 400 }
    );
  }

  // Rate limiting: ensure at least 1 second between requests
  const now = Date.now();
  const timeSinceLastRequest = now - lastRequestTime;
  if (timeSinceLastRequest < MIN_REQUEST_INTERVAL) {
    const waitTime = MIN_REQUEST_INTERVAL - timeSinceLastRequest;
    await new Promise(resolve => setTimeout(resolve, waitTime));
  }
  lastRequestTime = Date.now();

  try {
    // Build query parameters according to NLB API specification
    const params = new URLSearchParams({
      APIKey: apiKey,
      Keywords: query,
      Limit: String(pageSize),
      SetNo: String(page),
    });

    // Add media code filter if specified
    if (mediaCode && mediaCode !== 'ALL') {
      params.append('MediaCode', mediaCode);
    }

    const url = `${NLB_API_BASE}/GetTitles?${params.toString()}`;
    console.log('Calling NLB API (key hidden)');

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'NLB-Catalogue-Integration/1.0',
      },
      // Add timeout
      signal: AbortSignal.timeout(10000), // 10 second timeout
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('NLB API Error:', response.status, errorText);
      
      // Handle specific error cases
      if (response.status === 401) {
        return NextResponse.json(
          { error: 'Invalid API key. Please check your NLB_API_KEY configuration.' },
          { status: 401 }
        );
      }
      
      if (response.status === 429) {
        return NextResponse.json(
          { error: 'Rate limit exceeded. Please try again in a moment.' },
          { status: 429 }
        );
      }

      throw new Error(`API returned ${response.status}: ${errorText}`);
    }

    const data = await response.json();
    console.log('NLB API Response received');

    // Handle NLB API response structure
    const titles = data.titles || data.Titles || [];
    const totalRecords = data.totalRecords || data.TotalRecords || 0;
    const hasMore = data.hasMore || data.HasMore || (titles.length === pageSize);

    // Transform NLB data to your Book interface
    const items = titles.map((nlbBook: any) => {
      const bid = nlbBook.BID || nlbBook.bid;
      const isbn = nlbBook.ISBN || nlbBook.isbn;
      const title = nlbBook.Title || nlbBook.title || 'Untitled';
      const author = nlbBook.Author || nlbBook.author || '';
      const subjects = nlbBook.Subjects || nlbBook.subjects || [];
      const publisher = nlbBook.Publisher || nlbBook.publisher || 'Unknown Publisher';
      const publishYear = nlbBook.PublishYear || nlbBook.publishYear || '';
      const summary = nlbBook.Summary || nlbBook.summary || '';

      return {
        id: `nlb-${bid}`,
        title,
        authors: author
          .split(';')
          .map((a: string) => a.trim())
          .filter((a: string) => a.length > 0),
        categories: Array.isArray(subjects) ? subjects : [],
        maturityRating: 'NOT_MATURE',
        thumbnail: isbn
          ? `https://catalogue.nlb.gov.sg/cover/${isbn}.jpg`
          : '/images/book-placeholder.png',
        infoLink: `https://catalogue.nlb.gov.sg/cgi-bin/spydus.exe/ENQ/WPAC/BIBENQ?SETLVL=1&BRN=${bid}`,
        canonicalVolumeLink: `https://catalogue.nlb.gov.sg/cgi-bin/spydus.exe/ENQ/WPAC/BIBENQ?SETLVL=1&BRN=${bid}`,
        bestLink: `https://catalogue.nlb.gov.sg/cgi-bin/spydus.exe/ENQ/WPAC/BIBENQ?SETLVL=1&BRN=${bid}`,
        description: summary || null,
        synopsis: `${publisher}${publishYear ? ` (${publishYear})` : ''}`.trim(),
        snippet: summary || '',
        buckets: determineNLBBuckets(subjects, title),
        nlb: {
          BID: bid,
          ISBN: isbn,
          MediaCode: nlbBook.MediaCode || nlbBook.mediaCode,
          CallNumber: nlbBook.CallNumber || nlbBook.callNumber,
          PublishYear: publishYear,
          Publisher: publisher,
        }
      };
    });

    return NextResponse.json({
      items,
      totalApprox: totalRecords,
      hasMore,
      count: items.length,
      source: 'nlb'
    });

  } catch (error) {
    console.error('NLB API Integration Error:', error);
    
    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        return NextResponse.json(
          { error: 'Request timeout. The NLB API took too long to respond.' },
          { status: 504 }
        );
      }
      
      return NextResponse.json(
        { 
          error: 'Failed to search NLB catalog',
          details: error.message,
          help: 'Make sure your NLB_API_KEY is valid. Check the API documentation at https://openweb.nlb.gov.sg/api/swagger/index.html?urls.primaryName=Catalogue'
        },
        { status: 500 }
      );
    }

    return NextResponse.json(
      { error: 'An unexpected error occurred' },
      { status: 500 }
    );
  }
}

// Helper function to categorize NLB books into your bucket system
function determineNLBBuckets(subjects: string[], title: string): string[] {
  const buckets: string[] = [];
  const subjectsLower = subjects.map(s => s.toLowerCase()).join(' ');
  const titleLower = title.toLowerCase();
  const combined = `${subjectsLower} ${titleLower}`;

  // Fiction detection
  if (combined.includes('fiction') || combined.includes('stories') || combined.includes('novel')) {
    if (combined.includes('juvenile') || combined.includes('children')) {
      buckets.push('juvenile_fiction');
    } else if (combined.includes('young adult') || combined.includes('teen')) {
      buckets.push('young_adult');
    }
  }

  // Non-fiction detection
  if (combined.includes('non-fiction') || combined.includes('nonfiction')) {
    buckets.push('juvenile_nonfiction');
  }

  // Early readers
  if (combined.includes('picture book') || combined.includes('board book') || 
      combined.includes('early reader') || combined.includes('beginner')) {
    buckets.push('early_readers');
  }

  // Middle grade
  if (combined.includes('middle grade') || combined.includes('ages 8-12') || 
      combined.includes('juvenile literature')) {
    buckets.push('middle_grade');
  }

  // Biography
  if (combined.includes('biography') || combined.includes('biographies') || 
      combined.includes('autobiography')) {
    buckets.push('biography');
  }

  // Poetry & Humor
  if (combined.includes('poetry') || combined.includes('poems') || 
      combined.includes('humor') || combined.includes('humour') || 
      combined.includes('jokes') || combined.includes('comic')) {
    buckets.push('poetry_humor');
  }

  // Education
  if (combined.includes('education') || combined.includes('textbook') || 
      combined.includes('study') || combined.includes('learning') ||
      combined.includes('reference')) {
    buckets.push('education');
  }

  // Literature
  if (combined.includes('literature') || combined.includes('classic')) {
    buckets.push('literature');
  }

  // Science & Nature
  if (combined.includes('science') || combined.includes('nature') || 
      combined.includes('animals') || combined.includes('space')) {
    buckets.push('science_nature');
  }

  // Default bucket if none matched
  if (buckets.length === 0) {
    if (combined.includes('young adult') || combined.includes('teen')) {
      buckets.push('young_adult');
    } else if (combined.includes('juvenile') || combined.includes('children')) {
      buckets.push('juvenile_fiction');
    } else {
      buckets.push('juvenile_other');
    }
  }

  return [...new Set(buckets)]; // Remove duplicates
}