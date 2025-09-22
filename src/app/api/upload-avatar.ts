// For Pages Router: pages/api/upload-avatar.ts
// For App Router: app/api/upload-avatar/route.ts

import { v2 as cloudinary } from 'cloudinary';
import { NextApiRequest, NextApiResponse } from 'next';
import { NextRequest } from 'next/server';

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// Types for the request body
interface UploadRequest {
  image: string;
  userId: string;
}

// For Pages Router (pages/api/upload-avatar.ts)
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { image, userId }: UploadRequest = req.body;

    if (!image || !userId) {
      return res.status(400).json({ error: 'Image and userId are required' });
    }

    // Upload to Cloudinary
    const uploadResponse = await cloudinary.uploader.upload(image, {
      folder: 'avatars',
      public_id: `avatar_${userId}`,
      overwrite: true,
      transformation: [
        { width: 200, height: 200, crop: 'fill', gravity: 'face' }
      ]
    });

    return res.status(200).json({
      url: uploadResponse.secure_url,
      public_id: uploadResponse.public_id
    });

  } catch (error: any) {
    console.error('Cloudinary upload error:', error);
    return res.status(500).json({ 
      error: 'Failed to upload image',
      details: error.message 
    });
  }
}

// For App Router (app/api/upload-avatar/route.ts)
export async function POST(request: NextRequest) {
  try {
    const { image, userId }: UploadRequest = await request.json();

    if (!image || !userId) {
      return Response.json({ error: 'Image and userId are required' }, { status: 400 });
    }

    // Upload to Cloudinary
    const uploadResponse = await cloudinary.uploader.upload(image, {
      folder: 'avatars',
      public_id: `avatar_${userId}`,
      overwrite: true,
      transformation: [
        { width: 200, height: 200, crop: 'fill', gravity: 'face' }
      ]
    });

    return Response.json({
      url: uploadResponse.secure_url,
      public_id: uploadResponse.public_id
    });

  } catch (error: any) {
    console.error('Cloudinary upload error:', error);
    return Response.json({ 
      error: 'Failed to upload image',
      details: error.message 
    }, { status: 500 });
  }
}