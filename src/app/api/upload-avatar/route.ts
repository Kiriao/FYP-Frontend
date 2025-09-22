// app/api/upload-avatar/route.ts
import { v2 as cloudinary } from 'cloudinary';
import { NextRequest } from 'next/server';

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

interface UploadRequest {
  image: string;
  userId: string;
}

export async function POST(request: NextRequest) {
  console.log('🚀 Upload API route called');
  
  try {
    // Parse request body
    const { image, userId }: UploadRequest = await request.json();
    console.log('📝 Request parsed, userId:', userId);
    console.log('🖼️ Image data length:', image?.length);

    if (!image || !userId) {
      console.log('❌ Missing image or userId');
      return Response.json({ error: 'Image and userId are required' }, { status: 400 });
    }

    // Check environment variables
    console.log('🔧 Checking Cloudinary config...');
    console.log('Cloud name:', process.env.CLOUDINARY_CLOUD_NAME ? '✅' : '❌');
    console.log('API key:', process.env.CLOUDINARY_API_KEY ? '✅' : '❌');
    console.log('API secret:', process.env.CLOUDINARY_API_SECRET ? '✅' : '❌');

    if (!process.env.CLOUDINARY_CLOUD_NAME || !process.env.CLOUDINARY_API_KEY || !process.env.CLOUDINARY_API_SECRET) {
      console.log('❌ Cloudinary configuration missing');
      return Response.json({ error: 'Cloudinary configuration missing' }, { status: 500 });
    }

    console.log('☁️ Starting Cloudinary upload...');
    
    // Upload to Cloudinary
    const uploadResponse = await cloudinary.uploader.upload(image, {
      folder: 'avatars',
      public_id: `avatar_${userId}`,
      overwrite: true,
      transformation: [
        { width: 200, height: 200, crop: 'fill', gravity: 'face' }
      ]
    });

    console.log('✅ Upload successful:', uploadResponse.secure_url);

    return Response.json({
      url: uploadResponse.secure_url,
      public_id: uploadResponse.public_id
    });

  } catch (error: any) {
    console.error('💥 Cloudinary upload error:', error);
    console.error('Error details:', {
      message: error.message,
      name: error.name,
      stack: error.stack,
    });
    
    return Response.json({ 
      error: 'Failed to upload image',
      details: error.message,
      errorType: error.name
    }, { status: 500 });
  }
}