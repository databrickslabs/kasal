from crewai.tools import BaseTool
from typing import Optional, Type, Tuple
from pydantic import BaseModel, Field
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
from ..schemas import GoogleSlidesToolOutput
from typing import Any

# Load environment variables
load_dotenv()

# Configure logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoogleSlidesInput(BaseModel):
    """Input schema for GoogleSlidesTool."""
    content: str = Field(..., description="The raw content to be converted into slides")
    title: str = Field(default="New Presentation", description="The title of the presentation")

class GoogleSlidesTool(BaseTool):
    name: str = "GoogleSlidesTool"
    description: str = "A tool for creating and managing Google Slides presentations"
    args_schema: Type[BaseModel] = GoogleSlidesInput
    slides_service: Any = Field(default=None, exclude=True)
    drive_service: Any = Field(default=None, exclude=True)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._initialize_services()

    def _initialize_services(self):
        """Initialize Google Slides and Drive services"""
        try:
            # Check if we're in test environment
            if os.getenv('TESTING') == 'true':
                return
                
            # Use environment variable for credentials
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')
            
            if not os.path.exists(credentials_path):
                logger.warning(f"Credentials file not found at {credentials_path}")
                return
                
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/presentations',
                       'https://www.googleapis.com/auth/drive']
            )
            
            self.slides_service = build('slides', 'v1', credentials=credentials)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            
        except Exception as e:
            logger.error(f"Error initializing Google services: {str(e)}")
            self.slides_service = None
            self.drive_service = None

    def _parse_slides_content(self, raw_content: str, presentation_title: str = "New Presentation") -> list:
        """Parse raw content into structured slides data"""
        logger.debug("Starting content parsing...")
        logger.debug(f"Raw content received: {raw_content[:200]}...")
        
        slides = []
        
        # Split content into lines and process
        lines = raw_content.strip().split('\n')
        
        current_slide = None
        current_content = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.lower().startswith('slide'):
                # Save previous slide if exists
                if current_slide:
                    # Extract title from the content if it exists
                    title_line = next((l for l in current_content if l.startswith('Title:')), None)
                    if title_line:
                        title = title_line[6:].strip()  # Remove 'Title:' prefix
                        content = '\n'.join(l for l in current_content if l != title_line)
                    else:
                        title = current_slide
                        content = '\n'.join(current_content)
                    slides.append((title, content))
                    current_content = []
                current_slide = line
            else:
                current_content.append(line)
        
        # Add the last slide
        if current_slide and current_content:
            # Extract title from the content if it exists
            title_line = next((l for l in current_content if l.startswith('Title:')), None)
            if title_line:
                title = title_line[6:].strip()  # Remove 'Title:' prefix
                content = '\n'.join(l for l in current_content if l != title_line)
            else:
                title = current_slide
                content = '\n'.join(current_content)
            slides.append((title, content))
        
        # Create an overview slide at the beginning
        overview_content = "Overview:\n\n"
        for slide_title, _ in slides:
            if 'Title:' in slide_title:
                title = slide_title.split('Title:', 1)[1].strip()
            else:
                title = slide_title.split(':', 1)[1].strip() if ':' in slide_title else slide_title
            overview_content += f"• {title}\n"
        
        # Insert overview slide at the beginning
        slides.insert(0, (presentation_title, overview_content))
        
        logger.info(f"Parsed {len(slides)} slides")
        return slides

    def _create_slide_content(self, presentation_id: str, slide_id: str, title: str, content: str) -> None:
        """Helper function to create a slide with title and content"""
        logger.debug(f"Creating content for slide {slide_id}")
        logger.debug(f"Title: {title}")
        logger.debug(f"Content: {content}")

        try:
            # Create shapes first
            shape_requests = [
                {
                    'createShape': {
                        'objectId': f'{slide_id}_title_box',
                        'shapeType': 'TEXT_BOX',
                        'elementProperties': {
                            'pageObjectId': slide_id,
                            'size': {
                                'width': {'magnitude': 600, 'unit': 'PT'},
                                'height': {'magnitude': 50, 'unit': 'PT'}
                            },
                            'transform': {
                                'scaleX': 1,
                                'scaleY': 1,
                                'translateX': 50,
                                'translateY': 50,
                                'unit': 'PT'
                            }
                        }
                    }
                },
                {
                    'createShape': {
                        'objectId': f'{slide_id}_content_box',
                        'shapeType': 'TEXT_BOX',
                        'elementProperties': {
                            'pageObjectId': slide_id,
                            'size': {
                                'width': {'magnitude': 600, 'unit': 'PT'},
                                'height': {'magnitude': 400, 'unit': 'PT'}
                            },
                            'transform': {
                                'scaleX': 1,
                                'scaleY': 1,
                                'translateX': 50,
                                'translateY': 120,
                                'unit': 'PT'
                            }
                        }
                    }
                }
            ]

            # Create the shapes
            self.slides_service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={'requests': shape_requests}
            ).execute()

            # Insert text if not empty
            text_requests = []
            if title:
                text_requests.append({
                    'insertText': {
                        'objectId': f'{slide_id}_title_box',
                        'text': title
                    }
                })

            if content:
                text_requests.append({
                    'insertText': {
                        'objectId': f'{slide_id}_content_box',
                        'text': content
                    }
                })

            if text_requests:
                # Insert the text
                self.slides_service.presentations().batchUpdate(
                    presentationId=presentation_id,
                    body={'requests': text_requests}
                ).execute()

                # Apply styling only to boxes that have text
                style_requests = []
                if title:
                    style_requests.append({
                        'updateTextStyle': {
                            'objectId': f'{slide_id}_title_box',
                            'style': {
                                'fontFamily': 'Montserrat',
                                'fontSize': {'magnitude': 24, 'unit': 'PT'},
                                'foregroundColor': {
                                    'opaqueColor': {
                                        'rgbColor': {
                                            'red': 0.0,
                                            'green': 0.0,
                                            'blue': 0.0
                                        }
                                    }
                                }
                            },
                            'textRange': {'type': 'ALL'},
                            'fields': 'foregroundColor,fontFamily,fontSize'
                        }
                    })

                if content:
                    style_requests.append({
                        'updateTextStyle': {
                            'objectId': f'{slide_id}_content_box',
                            'style': {
                                'fontFamily': 'Montserrat',
                                'fontSize': {'magnitude': 14, 'unit': 'PT'},
                                'foregroundColor': {
                                    'opaqueColor': {
                                        'rgbColor': {
                                            'red': 0.0,
                                            'green': 0.0,
                                            'blue': 0.0
                                        }
                                    }
                                }
                            },
                            'textRange': {'type': 'ALL'},
                            'fields': 'foregroundColor,fontFamily,fontSize'
                        }
                    })

                if style_requests:
                    # Apply the styling
                    self.slides_service.presentations().batchUpdate(
                        presentationId=presentation_id,
                        body={'requests': style_requests}
                    ).execute()

        except Exception as e:
            logger.error(f"Error creating slide content: {str(e)}", exc_info=True)
            raise

    def _create_presentation_slides(self, content: str, title: str = "New Presentation") -> Tuple[str, str]:
        """Create a new presentation with slides"""
        logger.debug(f"Starting create_presentation_slides with title: {title}")
        logger.debug(f"Content received: {content[:200]}...")
        
        if None in (self.slides_service, self.drive_service):
            logger.error("Services not properly initialized")
            raise ValueError("Google Slides services not available")
            
        try:
            # Create a new presentation
            logger.debug("Creating new presentation...")
            presentation = self.slides_service.presentations().create(
                body={'title': title}
            ).execute()
            presentation_id = presentation.get('presentationId')
            logger.info(f"Created presentation with ID: {presentation_id}")
            
            # Get the list of existing slides
            presentation_details = self.slides_service.presentations().get(
                presentationId=presentation_id
            ).execute()
            existing_slides = presentation_details.get('slides', [])
            
            # Delete all existing slides if any exist
            if existing_slides:
                logger.debug(f"Deleting {len(existing_slides)} existing slides...")
                delete_requests = [{
                    'deleteObject': {
                        'objectId': slide.get('objectId')
                    }
                } for slide in existing_slides]
                
                if delete_requests:  # Only execute if there are requests
                    self.slides_service.presentations().batchUpdate(
                        presentationId=presentation_id,
                        body={'requests': delete_requests}
                    ).execute()
                    logger.info("Deleted all existing slides")
            
            # Share the presentation with specified email
            share_email = os.getenv('GOOGLE_SLIDES_SHARE_EMAIL', "nehmetohme@gmail.com")
            logger.debug(f"Sharing presentation with: {share_email}")
            try:
                self.drive_service.permissions().create(
                    fileId=presentation_id,
                    body={
                        'type': 'user',
                        'role': 'writer',
                        'emailAddress': share_email
                    },
                    sendNotificationEmail=True
                ).execute()
                logger.info(f"Successfully shared presentation with {share_email}")
            except Exception as e:
                logger.error(f"Error sharing presentation: {str(e)}")
            
            # Parse the content into slides
            logger.debug("Parsing content into slides...")
            slides_data = self._parse_slides_content(raw_content=content, presentation_title=title)
            logger.info(f"Parsed {len(slides_data)} slides")
            
            first_slide_id = None
            
            # Create each slide
            for index, (slide_title, slide_content) in enumerate(slides_data):
                logger.debug(f"Creating slide {index + 1}: {slide_title[:50]}...")
                
                # Create a new slide
                create_slide_request = {
                    'createSlide': {
                        'objectId': f'slide_{index}',
                        'insertionIndex': index,
                        'slideLayoutReference': {
                            'predefinedLayout': 'BLANK'
                        }
                    }
                }
                
                response = self.slides_service.presentations().batchUpdate(
                    presentationId=presentation_id,
                    body={'requests': [create_slide_request]}
                ).execute()
                
                # Get the slide ID
                slide_id = response.get('replies', [{}])[0].get('createSlide', {}).get('objectId')
                if index == 0:
                    first_slide_id = slide_id
                
                # Add content to the slide
                self._create_slide_content(presentation_id, slide_id, slide_title, slide_content)
                logger.debug(f"Content added to slide {index + 1}")
            
            # Generate the presentation link
            presentation_link = f"https://docs.google.com/presentation/d/{presentation_id}/edit"
            logger.info(f"Presentation created successfully: {presentation_link}")
            
            return presentation_id, first_slide_id
            
        except Exception as e:
            logger.error(f"Error creating presentation: {str(e)}", exc_info=True)
            raise

    def _share_presentation(self, presentation_id: str, email: str):
        """Share the presentation with specified email"""
        try:
            self.drive_service.permissions().create(
                fileId=presentation_id,
                body={
                    'type': 'user',
                    'role': 'writer',
                    'emailAddress': email
                },
                sendNotificationEmail=False
            ).execute()
            logger.info(f"Shared presentation with {email}")
        except Exception as e:
            logger.error(f"Error sharing presentation: {str(e)}")

    def _create_new_presentation(self, title: str = "New Presentation") -> str:
        """Create a new presentation and return its ID"""
        presentation = {
            'title': title
        }
        
        presentation = self.slides_service.presentations().create(body=presentation).execute()
        presentation_id = presentation.get('presentationId')
        logger.info(f'Created presentation with ID: {presentation_id}')
        
        return presentation_id

    def _run(self, content: str, title: str = "New Presentation") -> GoogleSlidesToolOutput:
        """Execute the Google Slides tool"""
        try:
            if None in (self.slides_service, self.drive_service):
                raise ValueError("Google Slides services not available")
            
            # Create the presentation and get IDs
            presentation_id, first_slide_id = self._create_presentation_slides(content, title)
            
            # Return the output using the schema
            return GoogleSlidesToolOutput(
                slide_id=first_slide_id,
                content=content,
                presentation_id=presentation_id
            )
            
        except Exception as e:
            error_msg = f"Error creating Google Slides presentation: {str(e)}"
            logger.error(error_msg)
            raise e